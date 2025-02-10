#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jan 28 17:58:35 2025

@author: gsimonet
"""

import pandas as pd
import xarray as xr
import numpy as np
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
def run_sequential_qc_pipeline(ds, season_thresholds, buddy_params, sct_params, min_completeness=0.8):
    """
    Runs quality control pipeline sequentially with completeness checks after each level.
    
    Parameters
    ----------
    ds : xarray.Dataset
        Dataset containing temperature and station data
    season_thresholds : dict
        Seasonal threshold parameters
    buddy_params : dict
        Parameters for buddy check
    sct_params : dict
        Parameters for spatial consistency test
    min_completeness : float
        Minimum completeness threshold
    
    Returns
    -------
    dict
        Dictionary containing all levels of QC and statistics
    """
    print("Starting sequential QC pipeline...")
    
    # Initialize results dictionary
    results = {
        'flags': {},
        'data': {},
        'masks': {},
        'statistics': {}
    }
    
    # Store original data
    results['data']['T_lvl0'] = ds.temperature.values.copy()
    
    # 1. Apply seasonal thresholds
    print("Level 1: Applying seasonal thresholds...")
    seasonal_flags = check_seasonal_thresholds(
        ds.temperature.values,
        ds.time.values,
        season_thresholds
    )
    
    # Store Level 1 results
    results['flags']['T_lvl1'] = seasonal_flags
    T_lvl1 = ds.temperature.values.copy()
    T_lvl1[~seasonal_flags] = np.nan
    results['data']['T_lvl1'] = T_lvl1

    # Apply completeness check after Level 1
    print("Checking completeness after seasonal thresholds...")
    completeness_flags_lvl1, completeness_stats_lvl1 = filter_by_completeness_temporal(
        T_lvl1,
        seasonal_flags,
        ds.time.values,
        min_completeness
    )
    
    # Update Level 1 data with completeness
    T_lvl1[~completeness_flags_lvl1] = np.nan
    results['data']['T_lvl1'] = T_lvl1
    results['flags']['T_lvl1'] = completeness_flags_lvl1
    results['statistics']['completeness_lvl1'] = completeness_stats_lvl1
    
    # 2. Run buddy checks on completeness-filtered Level 1 data
    print("Level 2: Running buddy checks...")
    buddy_flags = np.ones_like(T_lvl1, dtype=bool)
    
    # Process each timestep
    for t in range(T_lvl1.shape[0]):
        # Only process timesteps with enough valid data
        if np.sum(~np.isnan(T_lvl1[t])) >= buddy_params['num_min']:
            buddy_flags[t] = ~buddy_check(
                ds.latitude.values,
                ds.longitude.values,
                ds.altitude.values,
                T_lvl1[t],
                **buddy_params
            )
    
    # Store Level 2 results
    T_lvl2 = T_lvl1.copy()
    T_lvl2[~buddy_flags] = np.nan
    results['data']['T_lvl2'] = T_lvl2
    results['flags']['T_lvl2'] = buddy_flags

    # Apply completeness check after Level 2
    print("Checking completeness after buddy checks...")
    completeness_flags_lvl2, completeness_stats_lvl2 = filter_by_completeness_temporal(
        T_lvl2,
        buddy_flags,
        ds.time.values,
        min_completeness
    )
    
    # Update Level 2 data with completeness
    T_lvl2[~completeness_flags_lvl2] = np.nan
    results['data']['T_lvl2'] = T_lvl2
    results['flags']['T_lvl2'] = completeness_flags_lvl2
    results['statistics']['completeness_lvl2'] = completeness_stats_lvl2
    
    # 3. Run spatial-temporal consistency test on completeness-filtered Level 2 data
    print("Level 3: Running spatial-temporal consistency test...")
    sct_flags = np.ones_like(T_lvl2, dtype=bool)
    temporal_flags = np.ones_like(T_lvl2, dtype=bool)
    
    # Create shifted arrays for temporal check
    prev_values = np.roll(T_lvl2, 1, axis=0)
    next_values = np.roll(T_lvl2, -1, axis=0)
    prev_values[0] = np.nan
    next_values[-1] = np.nan
    
    # Process each timestep
    for t in range(T_lvl2.shape[0]):
        # Only process timesteps with enough valid data
        if np.sum(~np.isnan(T_lvl2[t])) >= sct_params['num_min']:
            flags, temp_flags = spatial_temporal_consistency_test(
                ds.latitude.values,
                ds.longitude.values,
                ds.altitude.values,
                T_lvl2[t],
                times=ds.time.values,
                prev_values=prev_values[t],
                next_values=next_values[t],
                **sct_params
            )
            sct_flags[t] = ~flags
            temporal_flags[t] = ~temp_flags
    
    # Combine flags
    combined_flags = sct_flags & temporal_flags
    
    # Store Level 3 results
    T_lvl3 = T_lvl2.copy()
    T_lvl3[~combined_flags] = np.nan
    results['data']['T_lvl3'] = T_lvl3
    results['flags']['T_lvl3'] = combined_flags

    # Apply completeness check after Level 3
    print("Checking completeness after spatial-temporal consistency test...")
    completeness_flags_lvl3, completeness_stats_lvl3 = filter_by_completeness_temporal(
        T_lvl3,
        combined_flags,
        ds.time.values,
        min_completeness
    )
    
    # Update Level 3 data with completeness
    T_lvl3[~completeness_flags_lvl3] = np.nan
    results['data']['T_lvl3'] = T_lvl3
    results['flags']['T_lvl3'] = completeness_flags_lvl3
    results['statistics']['completeness_lvl3'] = completeness_stats_lvl3
    
    # Calculate statistics
    results['statistics']['completeness'] = results['statistics']['completeness_lvl3']  # Use final level completeness
    results['statistics'].update({
        'total_values': np.prod(ds.temperature.shape),
        'values_per_level': {
            'T_lvl0': np.sum(~np.isnan(results['data']['T_lvl0'])),
            'T_lvl1': np.sum(~np.isnan(results['data']['T_lvl1'])),
            'T_lvl2': np.sum(~np.isnan(results['data']['T_lvl2'])),
            'T_lvl3': np.sum(~np.isnan(results['data']['T_lvl3']))
        },
        'seasonal_flags': np.sum(~seasonal_flags),
        'buddy_flags': np.sum(~buddy_flags),
        'sct_flags': np.sum(~sct_flags),
        'temporal_flags': np.sum(~temporal_flags)
    })
    
    print("Sequential QC pipeline completed.")
    return results

# def create_multilevel_netcdf(ds, qc_results, output_path):
#     """Creates NetCDF file with all QC levels."""
#     # Create dataset with all QC levels
#     ds_levels = xr.Dataset()
    
#     # Add original and level 1 data (full dimensions)
#     for level in ['T_lvl0', 'T_lvl1']:
#         ds_levels[level] = xr.DataArray(
#             qc_results['data'][level],
#             dims=('time', 'station'),
#             coords={
#                 'time': ds.time.values,
#                 'station': ds.station.values
#             }
#         )
    
#     # Add level 2 data (filtered timesteps)
#     timestep_mask = qc_results['masks']['timestep_mask']
#     ds_levels['T_lvl2'] = xr.DataArray(
#         qc_results['data']['T_lvl2'],
#         dims=('time_filtered', 'station'),
#         coords={
#             'time_filtered': ds.time.values[timestep_mask],
#             'station': ds.station.values
#         }
#     )
    
#     # For levels 3 and 4, keep original station dimension
#     for level in ['T_lvl3', 'T_lvl4']:
#         ds_levels[level] = xr.DataArray(
#             qc_results['data'][level],
#             dims=('time_filtered', 'station'),
#             coords={
#                 'time_filtered': ds.time.values[timestep_mask],
#                 'station': ds.station.values
#             }
#         )
    
#     # Add level 5 data (filtered stations)
#     station_mask = qc_results['masks']['station_mask']
#     ds_levels['T_lvl5'] = xr.DataArray(
#         qc_results['data']['T_lvl5'],
#         dims=('time_filtered', 'station_filtered'),
#         coords={
#             'time_filtered': ds.time.values[timestep_mask],
#             'station_filtered': ds.station.values[station_mask]
#         }
#     )
    
#     # Add station metadata for all stations
#     ds_levels['latitude'] = ('station', ds.latitude.values)
#     ds_levels['longitude'] = ('station', ds.longitude.values)
#     ds_levels['altitude'] = ('station', ds.altitude.values)
    
#     # Add filtered station metadata
#     ds_levels['latitude_filtered'] = ('station_filtered', ds.latitude.values[station_mask])
#     ds_levels['longitude_filtered'] = ('station_filtered', ds.longitude.values[station_mask])
#     ds_levels['altitude_filtered'] = ('station_filtered', ds.altitude.values[station_mask])
    
#     # Add flags as separate variables
#     for level, flags in qc_results['flags'].items():
#         flag_name = f"{level}_flags"
#         if level in ['T_lvl1']:
#             # Full dimensions
#             ds_levels[flag_name] = xr.DataArray(
#                 flags.astype(int),
#                 dims=('time', 'station')
#             )
#         elif level in ['T_lvl2', 'T_lvl3', 'T_lvl4']:
#             # Filtered time dimension
#             ds_levels[flag_name] = xr.DataArray(
#                 flags.astype(int),
#                 dims=('time_filtered', 'station')
#             )
    
#     # Add metadata
#     ds_levels.attrs = {
#         'title': 'Multi-level quality controlled temperature data',
#         'creation_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
#         'qc_levels_description': (
#             'Level 0: Original data, '
#             'Level 1: Seasonal thresholds, '
#             'Level 2: Timestep filtering, '
#             'Level 3: Buddy check, '
#             'Level 4: Spatial consistency, '
#             'Level 5: Station filtering'
#         )
#     }
    
#     # Add variable attributes
#     for level in qc_results['data'].keys():
#         ds_levels[level].attrs = {
#             'units': '°C',
#             'standard_name': 'air_temperature',
#             'long_name': f'Air temperature - QC {level}'
#         }
    
#     ds_levels.to_netcdf(output_path)
#     return ds_levels

def create_multilevel_netcdf(ds, qc_results, output_path):
    """
    Creates NetCDF file with all QC levels, adapted for the new completeness filtering.
    
    Parameters
    ----------
    ds : xarray.Dataset
        Original dataset
    qc_results : dict
        Results from QC pipeline including flags and data for each level
    output_path : str or Path
        Path to save the output NetCDF file
    
    Returns
    -------
    xarray.Dataset
        Dataset containing all QC levels
    """
    import xarray as xr
    from datetime import datetime
    
    # Create dataset with all QC levels
    ds_levels = xr.Dataset()
    
    # Add data for all levels
    for level in qc_results['data'].keys():
        ds_levels[level] = xr.DataArray(
            qc_results['data'][level],
            dims=('time', 'station'),
            coords={
                'time': ds.time.values,
                'station': ds.station.values
            }
        )
        
        # Add corresponding flags if they exist
        if level in qc_results['flags']:
            flag_name = f"{level}_flags"
            ds_levels[flag_name] = xr.DataArray(
                qc_results['flags'][level].astype(int),
                dims=('time', 'station'),
                coords={
                    'time': ds.time.values,
                    'station': ds.station.values
                }
            )
    
    # Add station metadata
    ds_levels['latitude'] = ('station', ds.latitude.values)
    ds_levels['longitude'] = ('station', ds.longitude.values)
    ds_levels['altitude'] = ('station', ds.altitude.values)
    
    # Add variable attributes
    for level in qc_results['data'].keys():
        ds_levels[level].attrs = {
            'units': '°C',
            'standard_name': 'air_temperature',
            'long_name': f'Air temperature - QC {level}'
        }
        
        if f"{level}_flags" in ds_levels:
            ds_levels[f"{level}_flags"].attrs = {
                'units': '1',
                'long_name': 'Quality control flag',
                'flag_values': '0, 1',
                'flag_meanings': 'failed_qc passed_qc'
            }
    
    # Add completeness statistics if available
    if 'completeness' in qc_results['statistics']:
        completeness_stats = qc_results['statistics']['completeness']
        ds_levels.attrs.update({
            'days_flagged': completeness_stats['days_flagged'],
            'months_flagged': completeness_stats['months_flagged'],
            'stations_with_flagged_days': completeness_stats['stations_with_flagged_days'],
            'stations_with_flagged_months': completeness_stats['stations_with_flagged_months']
        })
    
    # Add general metadata
    ds_levels.attrs.update({
        'title': 'Multi-level quality controlled temperature data',
        'creation_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'qc_levels_description': (
            'Level 0: Original data, '
            'Level 1: Seasonal thresholds, '
            'Level 2: Completeness filtering (daily/monthly), '
            'Level 3: Buddy check, '
            'Level 4: Spatial-temporal consistency'
        ),
        'total_values': qc_results['statistics']['total_values']
    })
    
    # Add per-level statistics
    for level, count in qc_results['statistics']['values_per_level'].items():
        ds_levels.attrs[f'{level}_valid_values'] = count
    
    # Save to netCDF file
    ds_levels.to_netcdf(output_path)
    
    return ds_levels
