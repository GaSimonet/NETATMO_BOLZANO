"""
Compare the full run_qc.py pipeline (seasonal -> timestep completeness -> buddy -> SCT
-> station completeness) without SCT, with the in-house SCT and with titanlib.sct.

The pipeline itself (filters.run_qc_pipeline) is run unchanged; only the SCT function
it calls is swapped.

    ~/mambaforge/envs/RETURN_py_env/bin/python compare_qc_pipeline.py
"""
import argparse
from pathlib import Path

import numpy as np
import pandas as pd
import xarray as xr

import src.quality_control.filters as filters
from compare_sct import run_titanlib, SCT_PARAMS, TITANLIB_THRESHOLDS

# Same values as run_qc.py
SEASON_THRESHOLDS = {
    'DJF': {'min': -30, 'max': 20},
    'MAM': {'min': -10, 'max': 30},
    'JJA': {'min': 0, 'max': 40},
    'SON': {'min': -10, 'max': 30}
}
BUDDY_PARAMS = {
    'radius': 5000,
    'num_min': 3,
    'threshold': 3,
    'max_elev_diff': 100,
    'elev_gradient': -0.0065
}
MIN_COMPLETENESS = 0.8


def cached_buddy_check():
    """Buddy check is identical in every variant, so compute it once per timestep."""
    original = filters.buddy_check
    cache = {}

    def wrapper(lats, lons, alts, values, **kwargs):
        key = np.asarray(values).tobytes()
        if key not in cache:
            cache[key] = original(lats, lons, alts, values, **kwargs)
        return cache[key]
    return wrapper


def titanlib_sct(pos, neg):
    """Drop-in replacement for filters.spatial_consistency_test (returns suspect flags)."""
    def wrapper(lats, lons, alts, values, **kwargs):
        return run_titanlib(np.asarray(lats, float), np.asarray(lons, float),
                            np.asarray(alts, float), np.asarray(values, float), pos, neg)
    return wrapper


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--file', default='raw_nc_files/NetAtmo_Bolzano_temperature_20251112.nc')
    parser.add_argument('--start', help='optional start date, e.g. 2024-01-01')
    parser.add_argument('--end', help='optional end date')
    args = parser.parse_args()

    ds = xr.open_dataset(args.file).sortby('time')
    if args.start or args.end:
        ds = ds.sel(time=slice(args.start, args.end))

    filters.buddy_check = cached_buddy_check()
    inhouse_sct = filters.spatial_consistency_test

    variants = {'no_sct': None, 'inhouse': inhouse_sct}
    variants.update({name: titanlib_sct(*pn) for name, pn in TITANLIB_THRESHOLDS.items()})

    results = {}
    for name, sct in variants.items():
        print(f"\n=== {name} ===")
        filters.spatial_consistency_test = sct
        results[name] = filters.run_qc_pipeline(ds, SEASON_THRESHOLDS, BUDDY_PARAMS,
                                                SCT_PARAMS if sct else None, MIN_COMPLETENESS)
    filters.spatial_consistency_test = inhouse_sct

    raw_valid = ~np.isnan(ds.temperature.values)
    n_raw = int(raw_valid.sum())

    summary = []
    for name, r in results.items():
        s = r['statistics']
        kept = r['flags'] & raw_valid
        summary.append({
            'variant': name,
            'sct_flags': int(s['sct_flags']),
            'buddy_flags': int(s['buddy_flags']),
            'timesteps_removed': int(s['timesteps_removed']),
            'stations_kept': int(r['station_mask'].sum()),
            'stations_removed': int(s['stations_removed']),
            'final_good_values': int(kept.sum()),
            'retained_%_of_raw_valid': 100 * kept.sum() / n_raw,
        })
    summary = pd.DataFrame(summary)

    # Differences in final output vs the pipeline without SCT
    base = results['no_sct']
    base_kept = base['flags'] & raw_valid
    diff = []
    for name, r in results.items():
        if name == 'no_sct':
            continue
        kept = r['flags'] & raw_valid
        diff.append({
            'variant': name,
            'kept_by_both': int(np.sum(base_kept & kept)),
            'kept_only_no_sct': int(np.sum(base_kept & ~kept)),
            'kept_only_variant': int(np.sum(~base_kept & kept)),
            'stations_dropped_vs_no_sct': list(ds.station.values[base['station_mask'] & ~r['station_mask']]),
            'stations_added_vs_no_sct': list(ds.station.values[~base['station_mask'] & r['station_mask']]),
        })
    diff = pd.DataFrame(diff)

    per_station = pd.DataFrame({
        'station': ds.station.values,
        'alt': ds.altitude.values,
        'raw_valid': raw_valid.sum(axis=0),
        **{f'{name}_kept': (r['flags'] & raw_valid).sum(axis=0) for name, r in results.items()},
        **{f'{name}_station_kept': r['station_mask'] for name, r in results.items()},
    })

    out_dir = Path('qc_output') / 'sct_comparison'
    out_dir.mkdir(parents=True, exist_ok=True)
    summary.to_csv(out_dir / 'pipeline_summary.csv', index=False)
    diff.to_csv(out_dir / 'pipeline_diff.csv', index=False)
    per_station.to_csv(out_dir / 'pipeline_per_station.csv', index=False)

    pd.set_option('display.width', 200)
    pd.set_option('display.max_colwidth', 80)
    print(f"\nFile: {args.file}  |  {len(ds.time)} timesteps x {len(ds.station)} stations"
          f"  |  raw valid values: {n_raw}")
    print(summary.round(2).to_string(index=False))
    print()
    print(diff[['variant', 'kept_by_both', 'kept_only_no_sct', 'kept_only_variant']].to_string(index=False))
    for _, row in diff.iterrows():
        print(f"{row['variant']}: stations dropped vs no_sct {len(row['stations_dropped_vs_no_sct'])}, "
              f"added {len(row['stations_added_vs_no_sct'])}")
    print(f"\nOutputs written to {out_dir}/")


if __name__ == '__main__':
    main()
