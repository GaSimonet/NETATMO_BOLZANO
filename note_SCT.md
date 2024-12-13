# Spatial Consistency Test (SCT) Documentation

## Overview
The Spatial Consistency Test (SCT) is a quality control method designed to identify suspicious temperature observations by comparing each station's measurements with its neighboring stations. The test evaluates whether a station's reading is statistically consistent with the spatial pattern of temperatures in its vicinity.

## Key Components

### Spatial Search Parameters
- **outer_radius**: The maximum distance within which neighboring stations are considered for comparison
- **inner_radius**: The radius within which stations are marked as processed after checking a station
- **num_min**: Minimum number of neighbors required to perform the check
- **num_max**: Maximum number of neighbors to use in the comparison

### Statistical Parameters
- **pos_threshold**: Positive deviation threshold (in standard deviations)
- **neg_threshold**: Negative deviation threshold (in standard deviations)
- **eps2**: Error variance parameter (default: 0.1)
- **prob_gross_error**: Probability of gross error (default: 0.1)
- **num_iterations**: Number of times to repeat the consistency check (default: 1)

## Algorithm Steps

1. **Initialization**
   - Create a spatial index (KD-tree) from station coordinates
   - Initialize arrays for flags and processed stations

2. **For Each Station**
   - Find all neighbors within outer_radius
   - If number of neighbors < num_min: skip station
   - If number of neighbors > num_max: select num_max closest neighbors

3. **Statistical Analysis**
   - Calculate background value (mean of neighboring stations)
   - Calculate deviation from background
   - Calculate analysis error (standard deviation of neighboring values)

4. **Flagging**
   - Flag station if:
     - deviation > pos_threshold * analysis_error
     - deviation < -neg_threshold * analysis_error

5. **Processing**
   - Mark station as processed
   - Mark all stations within inner_radius as processed
   - Continue until all stations are processed

## Radius Relationships

### Effect of Different Radius Settings

1. **Large outer_radius with small inner_radius**
   - More dense sampling
   - Larger neighbor pool
   - Better for local anomaly detection
   ```
   Example:
   inner_radius = 5km
   outer_radius = 15km
   ```

2. **Similar inner and outer radii**
   - Sparse sampling
   - Smaller neighbor pool
   - Better for regional anomaly detection
   ```
   Example:
   inner_radius = 8km
   outer_radius = 10km
   ```

### Best Practices
- outer_radius should ensure sufficient neighbors (≥ num_min)
- inner_radius typically 1/2 to 2/3 of outer_radius
- Consider local station density when setting radii
- Balance between detection sensitivity and false positives

## Usage Example

```python
# Example parameters
params = {
    'inner_radius': 5000,  # 5km
    'outer_radius': 10000, # 10km
    'num_min': 3,
    'num_max': 10,
    'pos_threshold': 3.0,
    'neg_threshold': 3.0,
    'num_iterations': 1
}

flags = spatial_consistency_test(
    lats=station_lats,
    lons=station_lons,
    alts=station_altitudes,
    values=temperature_values,
    **params
)
```

## Notes
- The test is sensitive to parameter choices
- Consider seasonal variations when setting thresholds
- May need adjustment based on terrain complexity
- More iterations can help refine results but increase computation time

## References

- Durre, I., et al. (2010): Comprehensive Automated Quality Assurance of Daily Surface Observations
