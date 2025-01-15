import pandas as pd
import xarray as xr
import numpy as np
from pathlib import Path
from src.quality_control.filters import (
   check_seasonal_thresholds,
   spatial_consistency_test,
   buddy_check,
   filter_by_completeness,
   run_qc_pipeline,
   create_filtered_netcdf
)

def select_timeframe(ds):
   """Interactive time selection using direct timestamps
   -->>> used for selecting a time frame for the quality control script
   """
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
           # Direct timestamp creation
           start_date = pd.Timestamp(input("Enter start date (YYYY-MM-DD HH:MM:SS): "))
           end_date = pd.Timestamp(input("Enter end date (YYYY-MM-DD HH:MM:SS): "))
           
           # Select time slice
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
       'JJA': {'min': 0, 'max': 40},
       'SON': {'min': -10, 'max': 30}
   }

   buddy_params = {
       'radius': 5000,      # 5km radius
       'num_min': 3,
       'threshold': 3,
       'max_elev_diff': 100,
       'elev_gradient': -0.0065
   }

   sct_params = {
       'inner_radius': 2000,
       'outer_radius': 8000,
       'num_min': 10,
       'num_max': 10,
       'pos_threshold': 0.5,
       'neg_threshold': 0.5,
       'min_elev_diff': 20,
       'min_horizontal_scale': 1000,
       'vertical_scale': 200,
       'eps2': 0.5,
       'prob_gross_error': 0.2
   }

   # Get path to raw NC files directory and list available files
   raw_nc_dir = Path(__file__).parent / "raw_nc_files"
   nc_files = list(raw_nc_dir.glob('*.nc'))

   if not nc_files:
       print("No NetCDF files found in raw_nc_files directory")
       return

   print("\nAvailable NetCDF files:")
   for i, file in enumerate(nc_files, 1):
       print(f"{i}. {file.name}")

   # Select file
   while True:
       try:
           choice = int(input("\nSelect a file number: "))
           if 1 <= choice <= len(nc_files):
               input_file = nc_files[choice - 1]
               break
           print("Invalid choice")
       except ValueError:
           print("Please enter a valid number")

   # Load dataset
   print(f"Loading dataset from {input_file}")
   ds_full = xr.open_dataset(input_file)
   
   # Time selection
   ds, timeframe_str = select_timeframe(ds_full)
   
   # Run QC pipeline
   print("\nRunning QC pipeline...")
   results = run_qc_pipeline(
       ds,
       season_thresholds,
       buddy_params,
       sct_params,
       min_completeness=0.8
   )

   # Create output filename with timeframe
   output_dir = Path('qc_output')
   output_dir.mkdir(exist_ok=True)
   
   # Create more detailed timeframe string with start and end dates
   if timeframe_str == "full":
       start_date = ds.time.values[0]
       end_date = ds.time.values[-1]
       timeframe_detail = f"{pd.Timestamp(start_date).strftime('%Y%m%d_%H%M')}_{pd.Timestamp(end_date).strftime('%Y%m%d_%H%M')}"
   else:
       timeframe_detail = timeframe_str

   output_file = output_dir / f'temperature_qc_filtered_{timeframe_detail}.nc'
   
   # Create filtered dataset
   print(f"\nCreating filtered dataset: {output_file}")
   ds_filtered = create_filtered_netcdf(ds, results, output_file, remove_empty=True)

   # Print summary
   print("\nQC Pipeline Results:")
   print(f"Total values: {results['statistics']['total_values']}")
   print(f"Good values after all checks: {results['statistics']['good_values']}")
   print(f"Stations removed: {results['statistics']['stations_removed']}")
   print(f"Timesteps removed: {results['statistics']['timesteps_removed']}")
   print(f"Values flagged by seasonal check: {results['statistics']['seasonal_flags']}")
   print(f"Values flagged by buddy check: {results['statistics']['buddy_flags']}")
   print(f"Values flagged by SCT: {results['statistics']['sct_flags']}")
   
   print("\nDataset Summary:")
   print(f"Original number of stations: {len(ds.station)}")
   print(f"Filtered number of stations: {len(ds_filtered.station)}")
   print(f"Percentage of valid data points: {100 * (~np.isnan(ds_filtered.temperature)).sum().values / np.prod(ds_filtered.temperature.shape):.2f}%")

if __name__ == "__main__":
   main()