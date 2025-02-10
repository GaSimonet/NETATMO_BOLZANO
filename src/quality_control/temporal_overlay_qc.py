#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Feb  3 17:15:29 2025

@author: gsimonet
"""
import xarray as xr
from pathlib import Path
from datetime import datetime
from src.quality_control.filters import (
   check_seasonal_thresholds,
   spatial_temporal_consistency_test,  # Changed from spatial_consistency_test
   buddy_check,
   filter_by_completeness_temporal,
   run_qc_pipeline,
   create_filtered_netcdf
)
from src.quality_control.temporal_qc import *

def long_term_temporal_check(values, times, station_id, **params):
    """
    Performs both moving window and seasonal checks.
    """
    # Extract parameters with defaults
    window_size = params.get('window_size', 30*24)
    z_score_threshold = params.get('z_score_threshold', 3)
    seasonal_window = params.get('seasonal_window', 15*24)
    min_samples = params.get('min_samples', 720)

    flags = np.ones_like(values, dtype=bool)
    diagnostics = {
        'station_id': station_id,
        'checks_performed': [],
        'anomalies_detected': {}
    }

    times_pd = pd.to_datetime(times)
    
    # 1. Moving Window Analysis (as before)
    print(f"Performing moving window analysis for station {station_id}...")
    for i in range(len(values)):
        window_start = times_pd[i] - pd.Timedelta(hours=window_size//2)
        window_end = times_pd[i] + pd.Timedelta(hours=window_size//2)
        
        window_mask = (times_pd >= window_start) & (times_pd <= window_end)
        window_values = values[window_mask]
        
        if len(window_values) >= min_samples:
            z_score = (values[i] - np.nanmean(window_values)) / np.nanstd(window_values)
            if abs(z_score) > z_score_threshold:
                flags[i] = False
                if 'z_score_violations' not in diagnostics['anomalies_detected']:
                    diagnostics['anomalies_detected']['z_score_violations'] = []
                diagnostics['anomalies_detected']['z_score_violations'].append({
                    'time': times_pd[i],
                    'value': values[i],
                    'z_score': z_score
                })

    # 2. Seasonal Analysis
    print("Analyzing seasonal patterns...")
    days_of_year = times_pd.dayofyear
    
    # Group data by day of year
    seasonal_stats = {}
    for doy in range(1, 367):
        # Get data for this day of year (±seasonal window)
        window_start = doy - seasonal_window/24  # Convert hours to days
        window_end = doy + seasonal_window/24
        
        # Handle wrap-around at year boundaries
        if window_start < 1:
            doy_mask = (days_of_year >= (366 + window_start)) | (days_of_year <= window_end)
        elif window_end > 366:
            doy_mask = (days_of_year >= window_start) | (days_of_year <= (window_end - 366))
        else:
            doy_mask = (days_of_year >= window_start) & (days_of_year <= window_end)
            
        seasonal_data = values[doy_mask]
        
        if len(seasonal_data) >= min_samples:
            seasonal_stats[doy] = {
                'mean': np.nanmean(seasonal_data),
                'std': np.nanstd(seasonal_data)
            }
    
    # Apply seasonal checks
    for i in range(len(values)):
        doy = days_of_year[i]
        if doy in seasonal_stats:
            stats = seasonal_stats[doy]
            z_score = (values[i] - stats['mean']) / stats['std']
            
            if abs(z_score) > z_score_threshold:
                flags[i] = False
                if 'seasonal_violations' not in diagnostics['anomalies_detected']:
                    diagnostics['anomalies_detected']['seasonal_violations'] = []
                diagnostics['anomalies_detected']['seasonal_violations'].append({
                    'time': times_pd[i],
                    'value': values[i],
                    'day_of_year': doy,
                    'seasonal_z_score': z_score
                })

    return flags, diagnostics
def has_sufficient_reference_data(reference_times, min_years):
    """Check if reference data spans enough years"""
    times = pd.to_datetime(reference_times)
    total_years = (times.max() - times.min()).days / 365.25
    return total_years >= min_years


def apply_long_term_temporal_check(values, times, stations, **params):
    """
    Apply temporal checks to temperature values.
    
    Parameters
    ----------
    values : array-like
        Temperature values (time, station)
    times : array-like
        Timestamps
    stations : array-like
        Station IDs
    params : dict
        Temporal check parameters
    """
    flags = np.ones_like(values, dtype=bool)
    all_diagnostics = {}
    
    for s in range(len(stations)):
        station_values = values[:, s]
        station_id = stations[s]
        
        station_flags, diagnostics = long_term_temporal_check(
            station_values,
            times,
            station_id,
            window_size=params.get('window_size', 30*24),
            seasonal_window=params.get('seasonal_window', 15*24),
            z_score_threshold=params.get('z_score_threshold', 3),
            trend_threshold=params.get('trend_threshold', 0.05),
            min_samples=params.get('min_samples', 720)
        )
        
        flags[:, s] = station_flags
        all_diagnostics[station_id] = diagnostics
        
    return flags, all_diagnostics

def apply_temporal_overlay(ds_qc, temporal_params=None):
    if temporal_params is None:
        temporal_params = {
            'window_size': 30*24,
            'seasonal_window': 15*24,
            'z_score_threshold': 3,
            'trend_threshold': 0.05,
            'min_samples': 720
        }
    
    # Get current maximum QC level
    qc_levels = [var for var in ds_qc.variables if var.startswith('T_lvl')]
    current_max = max([int(var.split('lvl')[1].split('_')[0]) for var in qc_levels])
    current_var = f'T_lvl{current_max}'
    
    # Apply temporal check
    temporal_flags, diagnostics = apply_long_term_temporal_check(
        ds_qc[current_var].values,
        ds_qc.time.values,
        ds_qc.station.values,
        **temporal_params
    )
    
    # Create output dataset with temporal level
    ds_temporal = ds_qc.copy()
    next_level = current_max + 1
    
    new_temp = ds_qc[current_var].values.copy()
    new_temp[~temporal_flags] = np.nan
    
    ds_temporal[f'T_lvl{next_level}'] = xr.DataArray(
        new_temp,
        dims=('time', 'station'),
        coords={'time': ds_qc.time, 'station': ds_qc.station}
    )
    
    ds_temporal[f'T_lvl{next_level}'].attrs = {
        'units': '°C',
        'standard_name': 'air_temperature',
        'long_name': f'Air temperature - QC level {next_level} (temporal)'
    }
    
    # Add temporal analysis metadata
    ds_temporal.attrs.update({
        'temporal_analysis_params': str(temporal_params),
        'temporal_qc_level': str(next_level),
        'creation_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    })
    
    return ds_temporal

def save_temporal_overlay(ds_temporal, output_path):
    """
    Saves dataset with temporal overlay to NetCDF file.
    
    Parameters
    ----------
    ds_temporal : xarray.Dataset
        Dataset with temporal QC overlay
    output_path : str or Path
        Path to save the output NetCDF file
    """
    # Create output directory if needed
    output_path = Path(output_path)
    output_path.parent.mkdir(exist_ok=True)
    
    # Save to NetCDF
    ds_temporal.to_netcdf(output_path)
    
    return output_path

#%% run it !
def main():
    # Set paths
    qc_input = '/home/gsimonet/Desktop/NETATMO_BOLZANO_PACKAGE/qc_output/temperature_qc_levels_20211231_2300_20250201_0000.nc'
    output_dir = Path('qc_output')
    
    # Load QC'd data
    ds_qc = xr.open_dataset(qc_input)
    
    # Define temporal parameters
    temporal_params = {
        'window_size': 30*24,  # 30 days
        'seasonal_window': 15*24,  # 15 days
        'z_score_threshold': 3,
        'trend_threshold': 0.05,
        'min_samples': 720
    }
    
    # Apply temporal overlay
    ds_temporal = apply_temporal_overlay(ds_qc, temporal_params)
    
    # Create output filename with timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M')
    output_file = output_dir / f'temperature_qc_temporal_{timestamp}.nc'
    
    # Save result
    save_temporal_overlay(ds_temporal, output_file)
    
    # Print summary
    current_level = max([int(var.split('lvl')[1].split('_')[0]) 
                        for var in ds_temporal.variables if var.startswith('T_lvl')])
    
    print(f"\nTemporal QC Summary:")
    print(f"Added level: T_lvl{current_level}")
    print(f"Total values: {ds_temporal[f'T_lvl{current_level}'].size}")
    print(f"Valid values: {(~np.isnan(ds_temporal[f'T_lvl{current_level}'])).sum()}")
    print(f"Output saved to: {output_file}")
if __name__ == "__main__":
    main()
