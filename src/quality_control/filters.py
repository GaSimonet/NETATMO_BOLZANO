import pandas as pd
import numpy as np
import cartopy.crs as ccrs
from scipy.spatial import cKDTree
import xarray as xr
from pathlib import Path
from datetime import datetime

def check_seasonal_thresholds(values, dates, season_thresholds):
    """Checks values against seasonal thresholds."""
    flags = np.ones_like(values, dtype=bool)
    months = pd.DatetimeIndex(dates).month
    
    seasons = pd.Series(months).map({
        12: 'DJF', 1: 'DJF', 2: 'DJF',
        3: 'MAM', 4: 'MAM', 5: 'MAM',
        6: 'JJA', 7: 'JJA', 8: 'JJA',
        9: 'SON', 10: 'SON', 11: 'SON'
    }).values
    
    for season, limits in season_thresholds.items():
        season_mask = (seasons == season)[:, np.newaxis]
        if not season_mask.any():
            continue
        if 'min' in limits:
            flags &= ~((values < limits['min']) & season_mask)
        if 'max' in limits:
            flags &= ~((values > limits['max']) & season_mask)
    
    return flags

def spatial_consistency_test(lats, lons, alts, values, inner_radius, outer_radius,
                           num_min, num_max, pos_threshold, neg_threshold,
                           min_elev_diff, min_horizontal_scale, vertical_scale,
                           eps2=0.1, prob_gross_error=0.1, num_iterations=1,
                           obs_to_check=None, tree=None):
    """Performs spatial consistency test on temperature observations."""
    if obs_to_check is None:
        obs_to_check = np.ones(len(values), dtype=bool)
        
    if tree is None:
        points = np.column_stack([lons, lats])
        tree = cKDTree(points)
    
    flags = np.zeros(len(values), dtype=bool)
    processed = np.zeros(len(values), dtype=bool)
    
    for _ in range(num_iterations):
        idx_to_process = np.where(obs_to_check & ~processed)[0]
        
        while len(idx_to_process) > 0:
            idx = idx_to_process[0]
            neighbors_idx = tree.query_ball_point([lons[idx], lats[idx]], outer_radius)
            neighbors_idx = [i for i in neighbors_idx if i != idx and not flags[i]]
            
            if len(neighbors_idx) < num_min:
                processed[idx] = True
                idx_to_process = idx_to_process[1:]
                continue
            
            if len(neighbors_idx) > num_max:
                distances = np.sqrt((lons[neighbors_idx] - lons[idx])**2 + 
                                 (lats[neighbors_idx] - lats[idx])**2)
                closest_idx = np.argsort(distances)[:num_max]
                neighbors_idx = [neighbors_idx[i] for i in closest_idx]
            
            background = np.mean(values[neighbors_idx])
            deviation = values[idx] - background
            analysis_error = np.std(values[neighbors_idx])
            
            if deviation > pos_threshold * analysis_error or deviation < -neg_threshold * analysis_error:
                flags[idx] = True
            
            processed[idx] = True
            inner_neighbors = tree.query_ball_point([lons[idx], lats[idx]], inner_radius)
            processed[inner_neighbors] = True
            idx_to_process = np.where(obs_to_check & ~processed)[0]
    
    return flags

def buddy_check(lats, lons, alts, values, radius, num_min=3, threshold=3, 
                max_elev_diff=-1, elev_gradient=0, min_std=0.1, num_iterations=1):
    """Performs buddy check on temperature observations."""
    proj = ccrs.UTM(33)
    pc = ccrs.PlateCarree()
    xy = proj.transform_points(pc, lons, lats)
    x, y = xy[:, 0], xy[:, 1]
    
    points = np.column_stack([x, y, alts])
    values = np.array(values)
    flags = np.zeros(len(values), dtype=bool)
    
    nan_mask = np.isnan(values)
    flags[nan_mask] = True
    
    tree = cKDTree(points[:, :2])
    
    for _ in range(num_iterations):
        neighbors_list = tree.query_ball_point(points[:, :2], radius)
        
        for i in range(len(values)):
            if nan_mask[i]:
                continue
                
            neighbors = np.array(neighbors_list[i])
            neighbors = neighbors[neighbors != i]
            neighbors = neighbors[~nan_mask[neighbors]]
            
            if max_elev_diff > 0:
                elev_diffs = np.abs(points[neighbors, 2] - points[i, 2])
                neighbors = neighbors[elev_diffs <= max_elev_diff]
            
            if len(neighbors) < num_min:
                flags[i] = True
                continue
            
            neighbor_values = values[neighbors]
            if elev_gradient != 0:
                elev_diffs = points[neighbors, 2] - points[i, 2]
                neighbor_values = neighbor_values + (elev_gradient * elev_diffs)
            
            mean = np.mean(neighbor_values)
            std = max(np.std(neighbor_values), min_std)
            
            if abs(values[i] - mean) > threshold * std:
                flags[i] = True
    
    return flags

def filter_by_completeness(data, flags, min_completeness=0.8, axis=1):
    """Filters timesteps or stations based on completeness threshold."""
    good_fraction = np.sum(flags, axis=axis) / flags.shape[axis]
    return good_fraction >= min_completeness

def run_qc_pipeline(ds, season_thresholds, buddy_params, sct_params, min_completeness=0.8):
    """
    Runs the complete quality control pipeline.
    
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
        Dictionary containing final flags and QC statistics
    """
    print("Starting QC pipeline...")
    
    # 1. Apply seasonal thresholds
    print("Applying seasonal thresholds...")
    seasonal_flags = check_seasonal_thresholds(
        ds.temperature.values,
        ds.time.values,
        season_thresholds
    )
    
    # 2. Filter timesteps by completeness
    print("Filtering timesteps...")
    timestep_mask = filter_by_completeness(
        ds.temperature.values,
        seasonal_flags,
        min_completeness,
        axis=1
    )
    
    # Apply timestep filtering
    filtered_data = ds.temperature.values[timestep_mask]
    filtered_dates = ds.time.values[timestep_mask]
    filtered_flags = seasonal_flags[timestep_mask]
    
    # 3. Run buddy checks
    print("Running buddy checks...")
    buddy_flags = np.zeros_like(filtered_data, dtype=bool)
    for t in range(filtered_data.shape[0]):
        buddy_flags[t] = buddy_check(
            ds.latitude.values,
            ds.longitude.values,
            ds.altitude.values,
            filtered_data[t],
            **buddy_params
        )
    
    # 4. Run spatial consistency test
    print("Running spatial consistency test...")
    sct_flags = np.zeros_like(filtered_data, dtype=bool)
    for t in range(filtered_data.shape[0]):
        sct_flags[t] = spatial_consistency_test(
            ds.latitude.values,
            ds.longitude.values,
            ds.altitude.values,
            filtered_data[t],
            **sct_params
        )
    
    # 5. Combine flags
    combined_flags = (
        filtered_flags & 
        ~buddy_flags &  # Invert because buddy_check returns suspect flags
        ~sct_flags      # Invert because SCT returns suspect flags
    )
    
    # 6. Filter by station completeness
    print("Filtering stations...")
    station_mask = filter_by_completeness(
        filtered_data,
        combined_flags,
        min_completeness,
        axis=0
    )
    
    # Create final flags array
    final_flags = np.zeros_like(ds.temperature.values, dtype=bool)
    final_flags_filtered = np.zeros((len(timestep_mask), ds.temperature.shape[1]), dtype=bool)
    final_flags_filtered[timestep_mask, :] = np.zeros((np.sum(timestep_mask), ds.temperature.shape[1]))
    final_flags_filtered[np.ix_(timestep_mask, station_mask)] = combined_flags[:, station_mask]
    
    # Calculate statistics
    stats = {
        'total_values': np.prod(ds.temperature.shape),
        'good_values': np.sum(final_flags_filtered),
        'stations_removed': np.sum(~station_mask),
        'timesteps_removed': np.sum(~timestep_mask),
        'seasonal_flags': np.sum(~seasonal_flags),
        'buddy_flags': np.sum(buddy_flags),
        'sct_flags': np.sum(sct_flags)
    }
    
    print("QC pipeline completed.")
    
    return {
        'flags': final_flags_filtered,
        'timestep_mask': timestep_mask,
        'station_mask': station_mask,
        'statistics': stats
    }

def create_filtered_netcdf(ds, qc_results, output_path, remove_empty=True):
    """Creates filtered NetCDF with QC results."""
    filtered_temp = ds.temperature.values.copy()
    filtered_temp[~qc_results['flags']] = np.nan
    
    if remove_empty:
        valid_stations = ~np.all(np.isnan(filtered_temp), axis=0)
        filtered_temp = filtered_temp[:, valid_stations]
        qc_flags = qc_results['flags'][:, valid_stations]
        stations = ds.station.values[valid_stations]
        lats = ds.latitude.values[valid_stations]
        lons = ds.longitude.values[valid_stations]
        alts = ds.altitude.values[valid_stations]
    else:
        qc_flags = qc_results['flags']
        stations = ds.station.values
        lats = ds.latitude.values
        lons = ds.longitude.values
        alts = ds.altitude.values
    
    ds_filtered = xr.Dataset(
        data_vars={
            'temperature': (('time', 'station'), filtered_temp),
            'temperature_qc': (('time', 'station'), qc_flags.astype(int)),
            'latitude': ('station', lats),
            'longitude': ('station', lons),
            'altitude': ('station', alts)
        },
        coords={
            'time': ds.time.values,
            'station': stations
        }
    )
    
    # Add metadata
    ds_filtered.temperature.attrs = {
        'units': '°C',
        'standard_name': 'air_temperature',
        'long_name': 'Quality controlled air temperature'
    }
    
    ds_filtered.temperature_qc.attrs = {
        'units': '1',
        'long_name': 'Quality control flag',
        'flag_values': '0, 1',
        'flag_meanings': 'failed_qc passed_qc'
    }
    
    ds_filtered.attrs = {
        'title': 'Quality controlled temperature data',
        'creation_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'qc_methods': 'Seasonal thresholds, buddy check, spatial consistency test'
    }
    
    ds_filtered.to_netcdf(output_path)
    return ds_filtered