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


def _project_utm(lats, lons):
    """Projects lat/lon to UTM coordinates in metres (same projection as buddy_check)."""
    xy = ccrs.UTM(33).transform_points(ccrs.PlateCarree(), np.asarray(lons), np.asarray(lats))
    return xy[:, 0], xy[:, 1]


def spatial_consistency_test(lats, lons, alts, values, radius=5000, num_min=5,
                             num_max=10, threshold=3.0, max_elev_diff=200,
                             elev_gradient=-0.0065, min_std=0.5, eps=100.0,
                             num_iterations=2):
    """
    Spatial consistency test (SCT) on temperature observations of one timestep.

    For each observation, a background is estimated by inverse-distance weighting of
    the num_max nearest valid neighbours within `radius`, after adjusting neighbour
    values to the station's elevation with `elev_gradient`. The observation is flagged
    if |value - background| > threshold * max(std of adjusted neighbours, min_std).
    Flagged observations are excluded as neighbours in the next iteration.

    Parameters
    ----------
    lats, lons, alts : array-like
        Station coordinates [deg] and elevations [m]
    values : array-like
        Temperatures [°C]; NaN values are ignored (never flagged, never used)
    radius : float
        Neighbour search radius [m]
    num_min, num_max : int
        Minimum neighbours needed to test an observation / maximum used
    threshold : float
        Allowed deviation in units of the neighbours' standard deviation
    max_elev_diff : float
        Neighbours differing more than this in elevation are ignored [m] (<= 0 disables)
    elev_gradient : float
        Lapse rate used for elevation adjustment [°C/m]
    min_std : float
        Lower bound on the standard deviation [°C]
    eps : float
        Distance added in the inverse-distance weights [m]
    num_iterations : int
        Number of passes

    Returns
    -------
    np.ndarray of bool
        True where the observation is suspect
    """
    alts = np.asarray(alts, dtype=float)
    values = np.asarray(values, dtype=float)
    n = len(values)
    flags = np.zeros(n, dtype=bool)

    x, y = _project_utm(lats, lons)
    tree = cKDTree(np.column_stack([x, y]))

    # Neighbour matrix (n x k) sorted by distance, padded with -1
    k = n if n <= 1 else n - 1
    dist, nb = tree.query(np.column_stack([x, y]), k=k + 1, distance_upper_bound=radius)
    dist, nb = dist[:, 1:], nb[:, 1:]          # drop self
    in_range = nb < n
    nb = np.where(in_range, nb, 0)
    if max_elev_diff > 0:
        in_range &= np.abs(alts[nb] - alts[:, None]) <= max_elev_diff

    # Neighbour values adjusted to the station's elevation
    adjusted = values[nb] + elev_gradient * (alts[:, None] - alts[nb])
    weights = 1.0 / (dist + eps)

    valid = ~np.isnan(values) & ~np.isnan(alts)
    for _ in range(num_iterations):
        usable = in_range & valid[nb] & ~flags[nb] & ~np.isnan(adjusted)
        usable &= np.cumsum(usable, axis=1) <= num_max
        count = usable.sum(axis=1)

        w = np.where(usable, weights, 0.0)
        adj = np.where(usable, adjusted, 0.0)
        with np.errstate(invalid='ignore', divide='ignore'):
            background = (w * adj).sum(axis=1) / w.sum(axis=1)
            mean = adj.sum(axis=1) / count
            std = np.sqrt((np.where(usable, adjusted - mean[:, None], 0.0) ** 2).sum(axis=1) / count)
        std = np.maximum(std, min_std)

        tested = valid & ~flags & (count >= num_min)
        new_flags = tested & (np.abs(values - background) > threshold * std)
        if not new_flags.any():
            break
        flags |= new_flags

    return flags


def spatial_temporal_consistency_test(lats, lons, alts, values, prev_values, next_values,
                                      temporal_threshold=3.0, **sct_kwargs):
    """
    Spatial consistency test combined with a step check against the previous and
    next timestep.

    Parameters
    ----------
    prev_values, next_values : array-like
        Observations at the previous / next timestep (NaN if unavailable)
    temporal_threshold : float
        Maximum allowed change between consecutive timesteps [°C]
    **sct_kwargs
        Parameters passed to spatial_consistency_test

    Returns
    -------
    flags : np.ndarray of bool
        True where the observation fails the spatial or the temporal check
    temporal_flags : np.ndarray of bool
        True where the observation fails the temporal check
    """
    values = np.asarray(values, dtype=float)
    with np.errstate(invalid='ignore'):
        temporal_flags = ((np.abs(values - prev_values) > temporal_threshold) |
                          (np.abs(values - next_values) > temporal_threshold))
    spatial_flags = spatial_consistency_test(lats, lons, alts, values, **sct_kwargs)
    return spatial_flags | temporal_flags, temporal_flags


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


## Deprecated (was too restrictive on long data sets)
def filter_by_completeness(data, flags, min_completeness=0.8, axis=1):
    """Filters timesteps or stations based on completeness threshold."""
    good_fraction = np.sum(flags, axis=axis) / flags.shape[axis]
    return good_fraction >= min_completeness


def filter_by_completeness_temporal(data, flags, times, min_completeness=0.8):
    """
    Filters data based on completeness at daily and monthly levels.
    
    Parameters
    ----------
    data : numpy.ndarray
        Input data array (timesteps x stations)
    flags : numpy.ndarray
        Boolean flags array of same shape as data
    times : numpy.ndarray
        Array of timestamps corresponding to data
    min_completeness : float
        Minimum completeness threshold (0-1)
        
    Returns
    -------
    numpy.ndarray
        Updated flags array with completeness filtering applied
    dict
        Statistics about filtering results
    """
    import pandas as pd
    import numpy as np
    
    # Convert times to pandas datetime if needed
    times_pd = pd.to_datetime(times)
    
    # Create DataFrame with dates for easier grouping
    dates_df = pd.DataFrame({
        'date': times_pd,
        'year': times_pd.year,
        'month': times_pd.month,
        'day': times_pd.day
    })
    
    # Initialize output flags as copy of input flags
    output_flags = flags.copy()
    
    stats = {
        'days_flagged': 0,
        'months_flagged': 0,
        'stations_with_flagged_days': 0,
        'stations_with_flagged_months': 0
    }
    
    # Process each station
    for station in range(data.shape[1]):
        station_data = data[:, station]
        # A value is valid only if it passed the QC and is not missing
        station_flags = flags[:, station] & ~np.isnan(station_data)
        output_flags[:, station] = station_flags
        
        # Create DataFrame for this station's data
        station_df = pd.DataFrame({
            'data': station_data,
            'flags': station_flags,
            'date': times_pd
        })
        
        # Daily completeness check
        daily_groups = station_df.groupby(station_df['date'].dt.date)
        days_flagged = False
        
        for day, group in daily_groups:
            # Calculate daily completeness
            expected_obs = 24  # Assuming hourly data
            valid_obs = np.sum(group['flags'])
            
            if valid_obs / expected_obs < min_completeness:
                # Flag all observations for this day
                output_flags[group.index, station] = False
                days_flagged = True
                stats['days_flagged'] += 1
        
        if days_flagged:
            stats['stations_with_flagged_days'] += 1
        
        # Monthly completeness check
        # Only perform if we have at least one month of data
        if (times_pd.max() - times_pd.min()).days >= 30:
            monthly_groups = station_df.groupby([station_df['date'].dt.year, 
                                               station_df['date'].dt.month])
            months_flagged = False
            
            for (year, month), group in monthly_groups:
                # Calculate monthly completeness based on daily flags
                days_in_month = pd.Period(year=year, month=month, freq='M').days_in_month
                expected_days = min(days_in_month, 
                                 len(pd.date_range(group['date'].min(), 
                                                 group['date'].max(), 
                                                 freq='D')))
                
                # Count days that passed the daily completeness check
                passed_daily = pd.Series(output_flags[group.index, station], index=group.index)
                valid_days = passed_daily.groupby(group['date'].dt.date).any().sum()
                
                if valid_days / expected_days < min_completeness:
                    # Flag all observations for this month
                    output_flags[group.index, station] = False
                    months_flagged = True
                    stats['months_flagged'] += 1
            
            if months_flagged:
                stats['stations_with_flagged_months'] += 1
    
    return output_flags, stats

def apply_completeness_filtering(ds, min_completeness=0.8):
    """
    Applies completeness filtering to a dataset.
    
    Parameters
    ----------
    ds : xarray.Dataset
        Input dataset with temperature data
    min_completeness : float
        Minimum completeness threshold
        
    Returns
    -------
    xarray.Dataset
        Filtered dataset
    dict
        Filtering statistics
    """
    import xarray as xr
    import numpy as np
    
    # Initial flags (all True)
    flags = np.ones_like(ds.temperature.values, dtype=bool)
    
    # Apply completeness filtering
    filtered_flags, stats = filter_by_completeness_temporal(
        ds.temperature.values,
        flags,
        ds.time.values,
        min_completeness
    )
    
    # Create masked dataset
    ds_filtered = ds.copy()
    ds_filtered['temperature'] = ds.temperature.where(filtered_flags)
    
    # Add QC flags as a variable
    ds_filtered['qc_flags'] = xr.DataArray(
        filtered_flags,
        dims=ds.temperature.dims,
        coords=ds.temperature.coords
    )
    
    # Add filtering info to attributes
    ds_filtered.attrs.update({
        'completeness_threshold': min_completeness,
        'days_flagged': stats['days_flagged'],
        'months_flagged': stats['months_flagged'],
        'stations_with_flagged_days': stats['stations_with_flagged_days'],
        'stations_with_flagged_months': stats['stations_with_flagged_months']
    })
    
    return ds_filtered, stats



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
    sct_params : dict or None
        Parameters for spatial consistency test (None skips the test)
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
    
    # 4. Run spatial consistency test (optional, skipped if sct_params is None)
    # Values already rejected by the seasonal or buddy check are not used as neighbours
    sct_flags = np.zeros_like(filtered_data, dtype=bool)
    if sct_params is not None:
        print("Running spatial consistency test...")
        sct_input = np.where(filtered_flags & ~buddy_flags, filtered_data, np.nan)
        for t in range(filtered_data.shape[0]):
            sct_flags[t] = spatial_consistency_test(
                ds.latitude.values,
                ds.longitude.values,
                ds.altitude.values,
                sct_input[t],
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