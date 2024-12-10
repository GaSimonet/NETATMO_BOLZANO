#!/usr/bin/env python3
import pandas as pd
import xarray as xr
import numpy as np
from pathlib import Path
import glob
from datetime import datetime


def get_season(month):
    """Return season for given month number"""
    if month in [12, 1, 2]:
        return 'DJF'  # Winter
    elif month in [3, 4, 5]:
        return 'MAM'  # Spring
    elif month in [6, 7, 8]:
        return 'JJA'  # Summer
    else:
        return 'SON'  # Fall

def create_seasonal_summary(ds):
    """Create summary of stations per season"""
    print("\nSeasonal Summary:")
    
    # Convert time to pandas datetime for easier season assignment
    times = pd.to_datetime(ds.time.values)
    
    # Create mask for each season
    season_masks = {
        'DJF': times.month.isin([12, 1, 2]),
        'MAM': times.month.isin([3, 4, 5]),
        'JJA': times.month.isin([6, 7, 8]),
        'SON': times.month.isin([9, 10, 11])
    }
    
    for season, mask in season_masks.items():
        if not any(mask):
            print(f"{season}: No data")
            continue
            
        # Get season data
        season_data = ds.temperature.values[mask, :]
        
        # Count stations with at least one valid measurement
        stations_with_data = np.sum(~np.all(np.isnan(season_data), axis=0))
        
        # Calculate percentage of measurements available
        total_possible = season_data.size
        available = np.sum(~np.isnan(season_data))
        completeness = (available / total_possible) * 100
        
        # Get date range for this season
        season_times = times[mask]
        
        print(f"\n{season} (from {season_times.min()} to {season_times.max()}):")
        print(f"  Active stations: {stations_with_data}")
        print(f"  Data completeness: {completeness:.1f}%")
        print(f"  Time points: {len(season_times)}")

def create_netcdf_from_csvs(csv_dir, output_file):
    """Simple function to combine CSV files into one NetCDF with proper data type handling"""
    print("Reading CSV files...")
    
    # Read all CSV files
    dfs = []
    for file in glob.glob(f"{csv_dir}/*.csv"):
        df = pd.read_csv(file, index_col=0)
        df.index = pd.to_datetime(df.index)
        # Convert altitude to float, replacing 'Unknown' with NaN
        df['altitude'] = pd.to_numeric(df['altitude'], errors='coerce')
        dfs.append(df)
    
    # Combine all data
    print("Combining data...")
    combined_df = pd.concat(dfs)
    
    # Pivot the data to get a time x station matrix
    print("Creating time x station matrix...")
    pivot_df = combined_df.pivot_table(
        index=combined_df.index,
        columns='station_ID',
        values='temperature'
    )
    
    # Get station metadata
    station_metadata = combined_df.groupby('station_ID').first()
    
    # Create xarray dataset
    ds = xr.Dataset(
        data_vars={
            'temperature': (['time', 'station'], pivot_df.values),
            'latitude': ('station', station_metadata['latitude'].values),
            'longitude': ('station', station_metadata['longitude'].values),
            'altitude': ('station', station_metadata['altitude'].values),
        },
        coords={
            'time': pivot_df.index,
            'station': pivot_df.columns,
        }
    )
    
    # Add attributes
    ds.temperature.attrs['units'] = 'celsius'
    ds.longitude.attrs['units'] = 'degrees_east'
    ds.latitude.attrs['units'] = 'degrees_north'
    ds.altitude.attrs['units'] = 'meters'
    
    # Print general summary
    print("\nDataset Summary:")
    print(f"Total Stations: {len(ds.station)}")
    print(f"Total Timepoints: {len(ds.time)}")
    print(f"Overall Time period: {ds.time[0].values} to {ds.time[-1].values}")
    print(f"Total missing values: {np.isnan(ds.temperature).sum().item()}")
    print(f"Stations missing altitude: {np.isnan(ds.altitude).sum().item()}")
    
    # Print seasonal summary
    create_seasonal_summary(ds)
    
    # Save to NetCDF
    print(f"\nSaving to {output_file}")
    ds.to_netcdf(output_file)
    
    return ds

if __name__ == "__main__":
   # Set paths
   csv_dir = Path(__file__).parent / "temperature_station_data"
   raw_nc_dir = Path(__file__).parent / "raw_nc_files"
   raw_nc_dir.mkdir(exist_ok=True)  # Create directory if it doesn't exist
   
   timestamp = datetime.now().strftime("%Y%m%d")
   output_file = raw_nc_dir / f'NetAtmo_Bolzano_temperature_{timestamp}.nc'
   
   # Create NetCDF file
   ds = create_netcdf_from_csvs(csv_dir, output_file)