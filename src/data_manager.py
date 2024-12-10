from pathlib import Path
import pandas as pd
import xarray as xr

class DataManager:
    def __init__(self):
        # Define directory structure
        self.data_dir = Path('data')
        self.raw_dir = self.data_dir / 'raw'
        self.processed_dir = self.data_dir / 'processed'
        self.qc_dir = self.data_dir / 'qc'
        
        # Create directories
        for dir_path in [self.raw_dir, self.processed_dir, self.qc_dir]:
            dir_path.mkdir(parents=True, exist_ok=True)

    def save_station_data(self, df, station_id):
        """Save individual station data"""
        station_file = self.raw_dir / f'station_{station_id}.csv'
        df.to_csv(station_file)

    def combine_to_netcdf(self, csv_pattern='station_*.csv'):
        """Combine all station CSVs into one NetCDF"""
        # Read all station files
        all_data = []
        for csv_file in self.raw_dir.glob(csv_pattern):
            df = pd.read_csv(csv_file)
            all_data.append(df)
        
        # Combine all data
        combined_df = pd.concat(all_data)
        
        # Convert to xarray
        ds = combined_df.set_index(['datetime', 'station_ID']).to_xarray()
        
        # Save unfiltered netcdf
        unfiltered_file = self.processed_dir / 'temperature_unfiltered.nc'
        ds.to_netcdf(unfiltered_file)
        
        return ds

    def save_filtered_data(self, ds, flags):
        """Save QC filtered data"""
        # Apply QC flags to create filtered dataset
        filtered_ds = ds.where(~flags.any(dim='flag_type'))
        
        # Save filtered netcdf
        filtered_file = self.qc_dir / 'temperature_filtered.nc'
        filtered_ds.to_netcdf(filtered_file)
        
        # Save QC flags
        flags_file = self.qc_dir / 'qc_flags.nc'
        flags.to_netcdf(flags_file)

    def get_data_summary(self):
        """Print summary of available data"""
        print("\nData Storage Summary:")
        print("\nRaw Data (CSV):")
        for file in self.raw_dir.glob('*.csv'):
            size = file.stat().st_size / (1024 * 1024)  # Convert to MB
            print(f"- {file.name}: {size:.1f}MB")
            
        print("\nProcessed Data (NetCDF):")
        for file in self.processed_dir.glob('*.nc'):
            size = file.stat().st_size / (1024 * 1024)
            print(f"- {file.name}: {size:.1f}MB")
            
        print("\nQC Data (NetCDF):")
        for file in self.qc_dir.glob('*.nc'):
            size = file.stat().st_size / (1024 * 1024)
            print(f"- {file.name}: {size:.1f}MB")