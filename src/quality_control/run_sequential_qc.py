import pandas as pd
import xarray as xr
import numpy as np
from pathlib import Path
from src.quality_control.filters import (
    check_seasonal_thresholds,
    spatial_temporal_consistency_test,  # Changed from spatial_consistency_test
    buddy_check,
    filter_by_completeness_temporal
)

from src.quality_control.sequential_qc import(run_sequential_qc_pipeline,
                                              create_multilevel_netcdf)
def select_timeframe(ds):
    """Interactive time selection using direct timestamps."""
    # Sort dataset by time
    ds_sorted = ds.sortby('time')
    
    print("\nAvailable date range in dataset:")
    print(f"Start: {ds_sorted.time.values[0]}")
    print(f"End: {ds_sorted.time.values[-1]}")
    
    print("\nSelect time range:")
    print("1. Use full dataset")
    print("2. Select specific dates")
    choice = input("Enter choice (1 or 2): ").strip()
    
    if choice == '1':
        return ds_sorted, "full"
        
    elif choice == '2':
        try:
            start_date = pd.Timestamp(input("Enter start date (YYYY-MM-DD HH:MM:SS): "))
            end_date = pd.Timestamp(input("Enter end date (YYYY-MM-DD HH:MM:SS): "))
            
            ds_subset = ds_sorted.sel(time=slice(start_date, end_date))
            timeframe_str = f"{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}"
            
            print(f"\nSelected period: {start_date} to {end_date}")
            print(f"Number of timesteps: {len(ds_subset.time)}")
            
            return ds_subset, timeframe_str
            
        except ValueError as e:
            print(f"\nError with date format: {e}")
            print("Using full dataset instead")
            return ds_sorted, "full"
    
    return ds_sorted, "full"
def main():
    # Define parameters
    season_thresholds = {
        'DJF': {'min': -30, 'max': 20},
        'MAM': {'min': -10, 'max': 30},
        'JJA': {'min': 0, 'max': 45},
        'SON': {'min': -10, 'max': 30}
    }

    buddy_params = {
        'radius': 5000,      # 5km radius
        'num_min': 3,
        'threshold': 3,
        'max_elev_diff': 400,
        'elev_gradient': -0.0065
    }

    # Updated parameters for spatial-temporal consistency test
    sct_params = {
        'inner_radius': 2000,  # 2km radius
        'outer_radius': 5000,  # 5km radius
        'num_min': 10,
        'num_max': 10,
        'pos_threshold': 0.5,
        'neg_threshold': 0.5,
        'min_elev_diff': 20,
        'max_elev_diff': 200,  # Added for new function
        'min_horizontal_scale': 1000,
        'vertical_scale': 200,
        'temporal_threshold': 3.0,  # Added for temporal check
        'eps': 0.1  # Added for distance weighting
    }

    # Set paths
    raw_nc_dir = Path('/home/gsimonet/Desktop/NETATMO_BOLZANO_PACKAGE/raw_nc_files')
    input_file = raw_nc_dir / 'NetAtmo_Bolzano_temperature_20250203.nc'

    # Load dataset
    print(f"Loading dataset from {input_file}")
    ds_full = xr.open_dataset(input_file)
    
    # Time selection
    ds, timeframe_str = select_timeframe(ds_full)
    
    # Run sequential QC pipeline
    print("\nRunning sequential QC pipeline...")
    results = run_sequential_qc_pipeline(
        ds,
        season_thresholds,
        buddy_params,
        sct_params,
        min_completeness=0.8
    )

    # Create output directory
    output_dir = Path('/home/gsimonet/Desktop/NETATMO_BOLZANO_PACKAGE/qc_output')
    output_dir.mkdir(exist_ok=True)
    
    # Create detailed timeframe string
    if timeframe_str == "full":
        start_date = ds.time.values[0]
        end_date = ds.time.values[-1]
        timeframe_detail = f"{pd.Timestamp(start_date).strftime('%Y%m%d_%H%M')}_{pd.Timestamp(end_date).strftime('%Y%m%d_%H%M')}"
    else:
        timeframe_detail = timeframe_str

    # Create output filename
    output_file = output_dir / f'temperature_qc_levels_{timeframe_detail}.nc'
    
    # Create multi-level dataset
    print(f"\nCreating multi-level dataset: {output_file}")
    ds_levels = create_multilevel_netcdf(ds, results, output_file)

    # Print summary statistics
    print("\nQC Pipeline Results:")
    print("\nNumber of valid values at each level:")
    for level, count in results['statistics']['values_per_level'].items():
        total = results['statistics']['total_values']
        percentage = 100 * count / total
        print(f"{level}: {count:,} ({percentage:.2f}%)")
    
    # Print completeness statistics
    print("\nCompleteness Statistics:")
    comp_stats = results['statistics']['completeness']
    print(f"Days flagged: {comp_stats['days_flagged']}")
    print(f"Months flagged: {comp_stats['months_flagged']}")
    print(f"Stations with flagged days: {comp_stats['stations_with_flagged_days']}")
    print(f"Stations with flagged months: {comp_stats['stations_with_flagged_months']}")
    
    # Print other QC statistics
    print("\nQC Flag Statistics:")
    print(f"Seasonal threshold flags: {results['statistics']['seasonal_flags']}")
    print(f"Buddy check flags: {results['statistics']['buddy_flags']}")
    print(f"Spatial-temporal consistency flags: {results['statistics']['sct_flags']}")
    print(f"Temporal consistency flags: {results['statistics']['temporal_flags']}")
    
    print("\nDataset dimensions at each level:")
    for level in results['data'].keys():
        shape = results['data'][level].shape
        print(f"{level}: {shape[0]} timesteps × {shape[1]} stations")

if __name__ == "__main__":
    main()