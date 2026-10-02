"""
Compare the in-house SCT (src/quality_control/filters.spatial_consistency_test,
as called by run_qc.py) with titanlib.sct on the same observations.

Run with an environment where titanlib is installed, e.g.:
    ~/mambaforge/envs/RETURN_py_env/bin/python compare_sct.py --n-steps 2000
"""
import argparse
from pathlib import Path

import numpy as np
import pandas as pd
import xarray as xr
import matplotlib.pyplot as plt
import titanlib

from src.quality_control.filters import spatial_consistency_test

# Same values as sct_params in run_qc.py
SCT_PARAMS = {
    'radius': 5000,
    'num_min': 5,
    'num_max': 10,
    'threshold': 3.0,
    'max_elev_diff': 200,
    'elev_gradient': -0.0065,
    'min_std': 0.5,
    'eps': 100.0,
    'num_iterations': 2
}

# titanlib.sct settings (thresholds are on the SCT score, not multiples of sigma)
TITANLIB_PARAMS = {
    'num_min': 5,
    'num_max': 10,
    'inner_radius': 2000,
    'outer_radius': 5000,
    'num_min_prof': 5,
    'min_elev_diff': 20,
    'min_horizontal_scale': 1000,
    'vertical_scale': 200,
    'eps2': 0.5,
}
TITANLIB_THRESHOLDS = {     # (pos, neg)
    'titanlib_pos4_neg8': (4.0, 8.0),
}


def run_inhouse(lats, lons, alts, values):
    """In-house SCT (filters.spatial_consistency_test)."""
    return spatial_consistency_test(lats, lons, alts, values, **SCT_PARAMS)


def run_titanlib(lats, lons, alts, values, pos, neg):
    """titanlib.sct on valid observations only (titanlib does not skip NaN values)."""
    flags = np.zeros(len(values), dtype=bool)
    valid = ~np.isnan(values) & ~np.isnan(alts)
    n = int(valid.sum())
    p = TITANLIB_PARAMS
    if n < p['num_min'] + 1:
        return flags
    points = titanlib.Points(lats[valid], lons[valid], alts[valid])
    ones = np.ones(n)
    out = titanlib.sct(
        points, values[valid],
        p['num_min'], p['num_max'], p['inner_radius'], p['outer_radius'],
        1, p['num_min_prof'], p['min_elev_diff'], p['min_horizontal_scale'],
        p['vertical_scale'], pos * ones, neg * ones, p['eps2'] * ones
    )
    flags[valid] = np.asarray(out[0]) == 1
    return flags


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--file', default='raw_nc_files/NetAtmo_Bolzano_temperature_20251112.nc')
    parser.add_argument('--n-steps', type=int, default=2000,
                        help='number of random timesteps to compare (0 = all)')
    parser.add_argument('--seed', type=int, default=0)
    args = parser.parse_args()

    ds = xr.open_dataset(args.file)
    lats = ds.latitude.values.astype(float)
    lons = ds.longitude.values.astype(float)
    alts = ds.altitude.values.astype(float)
    temps = ds.temperature.values
    times = pd.to_datetime(ds.time.values)

    steps = np.arange(len(times))
    if 0 < args.n_steps < len(steps):
        steps = np.sort(np.random.default_rng(args.seed).choice(steps, args.n_steps, replace=False))

    methods = ['inhouse'] + list(TITANLIB_THRESHOLDS)
    flags = {m: np.zeros((len(steps), len(lats)), dtype=bool) for m in methods}
    valid = ~np.isnan(temps[steps])

    for k, t in enumerate(steps):
        values = temps[t].astype(float)
        flags['inhouse'][k] = run_inhouse(lats, lons, alts, values)
        for name, (pos, neg) in TITANLIB_THRESHOLDS.items():
            flags[name][k] = run_titanlib(lats, lons, alts, values, pos, neg)
        if (k + 1) % 500 == 0:
            print(f"  {k + 1}/{len(steps)} timesteps")

    # Only valid observations count (NaN can't be a flagged value in the pipeline output)
    n_valid = int(valid.sum())
    summary = []
    for m in methods:
        f = flags[m] & valid
        per_step = f.sum(axis=1)
        summary.append({
            'method': m,
            'flagged_obs': int(f.sum()),
            'flag_rate_%': 100 * f.sum() / n_valid,
            'max_flags_per_step': int(per_step.max()),
            'steps_with_any_flag_%': 100 * np.mean(per_step > 0),
            'stations_ever_flagged': int(f.any(axis=0).sum()),
        })
    summary = pd.DataFrame(summary)

    # Agreement of in-house vs titanlib on valid observations
    agreement = []
    a = flags['inhouse'][valid]
    for m in TITANLIB_THRESHOLDS:
        b = flags[m][valid]
        agreement.append({
            'vs': m,
            'both_flag': int(np.sum(a & b)),
            'inhouse_only': int(np.sum(a & ~b)),
            'titanlib_only': int(np.sum(~a & b)),
            'neither': int(np.sum(~a & ~b)),
            'jaccard': np.sum(a & b) / max(np.sum(a | b), 1),
        })
    agreement = pd.DataFrame(agreement)

    per_station = pd.DataFrame({
        'station': ds.station.values,
        'lat': lats, 'lon': lons, 'alt': alts,
        'n_valid': valid.sum(axis=0),
        **{f'{m}_flag_rate_%': 100 * (flags[m] & valid).sum(axis=0) / np.maximum(valid.sum(axis=0), 1)
           for m in methods},
    })

    out_dir = Path('qc_output') / 'sct_comparison'
    out_dir.mkdir(parents=True, exist_ok=True)
    summary.to_csv(out_dir / 'summary.csv', index=False)
    agreement.to_csv(out_dir / 'agreement.csv', index=False)
    per_station.to_csv(out_dir / 'per_station.csv', index=False)

    pd.set_option('display.width', 160)
    print(f"\nFile: {args.file}")
    print(f"Timesteps compared: {len(steps)}  |  valid observations: {n_valid}")
    print(f"titanlib version: {titanlib.version()}\n")
    print(summary.round(3).to_string(index=False))
    print()
    print(agreement.round(3).to_string(index=False))

    # Figure: per-station flag rate on a map, one panel per method
    fig, axes = plt.subplots(1, len(methods), figsize=(5 * len(methods), 4.5), sharex=True, sharey=True)
    vmax = max(per_station[f'{m}_flag_rate_%'].max() for m in methods) or 1
    for ax, m in zip(axes, methods):
        sc = ax.scatter(lons, lats, c=per_station[f'{m}_flag_rate_%'], s=30 + alts / 10,
                        cmap='viridis', vmin=0, vmax=vmax, edgecolor='k', linewidth=0.3)
        ax.set_title(f"{m}\n{summary.set_index('method').loc[m, 'flag_rate_%']:.2f}% of obs flagged")
        ax.set_xlabel('Longitude')
    axes[0].set_ylabel('Latitude')
    fig.colorbar(sc, ax=axes, label='Station flag rate (%)')
    fig.savefig(out_dir / 'flag_rate_map.png', dpi=150, bbox_inches='tight')
    print(f"\nOutputs written to {out_dir}/")


if __name__ == '__main__':
    main()
