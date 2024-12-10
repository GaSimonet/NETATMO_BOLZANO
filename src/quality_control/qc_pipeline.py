#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Quality Control Pipeline for Netatmo temperature data
"""
import os
import sys
from glob import glob
from pathlib import Path
import pandas as pd
import numpy as np
from datetime import datetime

# Add project directory to Python path
project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_dir)

from qc.basic_range_test import basic_range_test
from qc.temporal_consistency import temporal_consistency_test
from qc.spatial_consistency import spatial_consistency_test
from qc.statistical_test import statistical_test

class QualityControl:
    def __init__(self, input_dir='temperature_station_data', output_dir='QC/output'):
        self.input_dir = Path(input_dir)
        self.output_dir = Path(output_dir)
        self.flags_dir = Path('QC/flags')
        self.summary_dir = Path('QC/summary')
        
        # Create necessary directories
        for directory in [self.output_dir, self.flags_dir, self.summary_dir]:
            directory.mkdir(parents=True, exist_ok=True)
    
    def list_available_data(self):
        """List available temperature data files."""
        data_files = glob(str(self.input_dir / 'temperature_data_*.csv'))
        
        if not data_files:
            print("No temperature data files found.")
            return None
        
        print("\nAvailable temperature data files:")
        for i, filepath in enumerate(data_files, 1):
            # Get basic file info
            filename = os.path.basename(filepath)
            filesize = os.path.getsize(filepath) / (1024 * 1024)  # Convert to MB
            
            # Read first and last timestamps
            df = pd.read_csv(filepath, nrows=1)
            first_date = pd.to_datetime(df.index[0])
            
            df = pd.read_csv(filepath, skiprows=lambda x: x > 0 and x < sum(1 for _ in open(filepath)) - 1)
            last_date = pd.to_datetime(df.index[-1])
            
            print(f"{i}. {filename}")
            print(f"   Size: {filesize:.1f}MB")
            print(f"   Period: {first_date.date()} to {last_date.date()}")
            print(f"   Stations: {len(df['station_ID'].unique())}")
        
        return data_files
    
    def load_data(self, file_path):
        """Load and prepare data for QC."""
        print(f"\nLoading data from {os.path.basename(file_path)}...")
        df = pd.read_csv(file_path)
        df['datetime'] = pd.to_datetime(df.index)
        df.set_index('datetime', inplace=True)
        return df
    
    def run_qc(self, data_file, config=None):
        """Run all QC tests on the data."""
        # Load default config if none provided
        if config is None:
            config = {
                'basic_range': {
                    'min_temp': -40,
                    'max_temp': 50
                },
                'temporal': {
                    'max_change': 10,  # maximum temperature change per hour
                    'window_size': 3   # hours for moving window
                },
                'spatial': {
                    'distance_threshold': 2000,  # meters
                    'elevation_threshold': 100,  # meters
                    'std_threshold': 3
                },
                'statistical': {
                    'zscore_threshold': 3
                }
            }
        
        # Load data
        df = self.load_data(data_file)
        
        print("\nRunning Quality Control tests...")
        
        # Run tests
        print("1. Basic Range Test")
        range_flags = basic_range_test(df, 
                                     config['basic_range']['min_temp'],
                                     config['basic_range']['max_temp'])
        
        print("2. Temporal Consistency Test")
        temporal_flags = temporal_consistency_test(df,
                                                 config['temporal']['max_change'],
                                                 config['temporal']['window_size'])
        
        print("3. Spatial Consistency Test")
        spatial_flags = spatial_consistency_test(df,
                                               config['spatial']['distance_threshold'],
                                               config['spatial']['elevation_threshold'],
                                               config['spatial']['std_threshold'])
        
        print("4. Statistical Test")
        statistical_flags = statistical_test(df,
                                          config['statistical']['zscore_threshold'])
        
        # Combine flags
        all_flags = pd.concat([range_flags, temporal_flags, spatial_flags, statistical_flags],
                            axis=1, keys=['range', 'temporal', 'spatial', 'statistical'])
        
        # Save results
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Save flags
        flags_file = self.flags_dir / f'qc_flags_{timestamp}.csv'
        all_flags.to_csv(flags_file)
        
        # Save QC'd data
        output_file = self.output_dir / f'qc_data_{timestamp}.csv'
        df.to_csv(output_file)
        
        # Create and save summary
        summary = self.create_summary(df, all_flags)
        summary_file = self.summary_dir / f'qc_summary_{timestamp}.csv'
        summary.to_csv(summary_file)
        
        print(f"\nQuality Control completed!")
        print(f"Results saved to:")
        print(f"- Flags: {flags_file}")
        print(f"- Data: {output_file}")
        print(f"- Summary: {summary_file}")
        
        return df, all_flags, summary
    
    def create_summary(self, df, flags):
        """Create summary statistics of QC results."""
        summary = pd.DataFrame()
        
        # Calculate statistics per station
        for station in df['station_ID'].unique():
            station_data = df[df['station_ID'] == station]
            station_flags = flags.loc[station_data.index]
            
            station_summary = {
                'total_measurements': len(station_data),
                'range_flags': station_flags['range'].sum(),
                'temporal_flags': station_flags['temporal'].sum(),
                'spatial_flags': station_flags['spatial'].sum(),
                'statistical_flags': station_flags['statistical'].sum(),
                'total_flags': station_flags.sum().sum(),
                'percent_flagged': (station_flags.sum().sum() / len(station_data)) * 100
            }
            
            summary = pd.concat([summary, 
                               pd.DataFrame(station_summary, index=[station])])
        
        return summary

def main():
    # Initialize QC
    qc = QualityControl()
    
    # List available data files
    data_files = qc.list_available_data()
    if not data_files:
        return
    
    # Let user select data file
    while True:
        try:
            choice = int(input("\nSelect a data file to process (enter number): "))
            if 1 <= choice <= len(data_files):
                selected_file = data_files[choice - 1]
                break
            print("Invalid choice. Please try again.")
        except ValueError:
            print("Please enter a valid number.")
    
    # Run QC
    qc.run_qc(selected_file)

if __name__ == "__main__":
    main()