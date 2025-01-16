# Netatmo Data Collection and Quality Control Pipeline

This repository contains tools for collecting temperature data from Netatmo weather stations and applying quality control to the collected data. Below is a step-by-step guide on how to use this pipeline.

## Prerequisites

- Python 3.x
- Netatmo API account and credentials
- Required Python packages (listed in `requirements.txt`) [NEED to be updated]

## Setup

1. Clone the repository:
```bash
git clone https://gitlab.inf.unibz.it/earth_observation_public/CCT/pnrr-return/netatmo_uhi.git

```

2. Install required packages:

I have myself created a environment for the NetAtmo data:
conda env create -f netatmo_py_env.yaml

Alternatively use mamba or conda for creating and manually installing the packages:
```bash
$mamba create -n netatmo_py_env python=3.10
$mamba activate netatmo_py_env
$mamba install xarray pandas numpy cartopy netcdf4 requests urllib3 tqdm pathlib typing plotly dataclasses matplotlib seaborn scipy geopandas shapely 
```
otherwise :
```bash
pip install -r requirements.txt
```


3. Configure Netatmo API credentials:
   - Create a Netatmo developer account at https://dev.netatmo.com/
   - Create a new application to get your credentials
   - Save your credentials in `tokens.json`:
     ```json
     {
         "client_id": "your_client_id",
         "client_secret": "your_client_secret",
         "refresh_token": "your_refresh_token"
     }
     ```
I will soon add the environment.yaml for easing the process 
## Data Collection Process

### Step 1: Get Station List


First the user has to determine the available station in an area by giving a couple of the two opposite corners cordinates

```bash
python3 station_manager.py
```

Run station_manager.py with the desired corner coordinates 
The ones suggested in brackets are centered around Bolzano city.

### Step 2: Fetch Temperature Data

Run the data fetching script:
```bash
python3 run_fetch.py
```

The script will:
- Ask you to select a station list file
- Start downloading data for each station
- Handle rate limits automatically by:
  1. Trying to refresh the access token
  2. Waiting for an hour if needed
- Save progress and allow resuming if interrupted
- Create CSV files with temperature data in `temperature_station_data` directory

## Output Files
run create_netcdf.py --> gather all the csv files from temperature_data_stations folder into one .nc file
```bash
python3 create_netcdf.py
```

## Quality Control Pipeline

After collecting the data, run the quality control process:

1.Check if all the data where successfully downloaded

2. Run the quality control pipeline:
```bash
python3 run_qc.py
```
- First you can choose if you want to filter the full period or only a specific time frame.
The QC pipeline includes several tests:

-Seasonal Threshold Test

Checks temperature against seasonal limits (DJF, MAM, JJA, SON)
Different min/max thresholds for each season


-Buddy Check

Compares station with neighbors within a radius
Considers elevation differences
Flags values that deviate too much from neighbors


-Spatial Consistency Test (SCT)

More sophisticated neighbor comparison
Uses inner and outer radius
Considers both horizontal and vertical distances


-Completeness Check

Filters based on data completeness threshold
Applied to both timesteps and stations

this script also create filter_qc_nc files in qc_output_folder




## Tests 

- A unittest has been setup to test the different function of this package:

API Interaction Testing

Tests authentication flow with Netatmo API
Verifies correct handling of API responses
Ensures rate limiting is working


Data Processing Verification

Validates temperature data fetching
Checks data formatting and storage
Confirms quality control filters work correctly


Error Handling

Tests network failure scenarios
Verifies token refresh mechanism
Ensures proper handling of missing data

## Troubleshooting

Common issues and solutions:

1. Rate Limit Exceeded
   - The script will automatically handle rate limits (500 queries per hour)
   - Let it run and it will resume automatically

2. Missing Data
   - Check station availability in the station list
   - Verify API credentials
   - Ensure internet connectivity

3. QC Pipeline Errors
   - Check input data format
   - Verify all required files are present


## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

This package is under the GNU General Public License (GPL), see LICENSE.txt for more information.

## Contact

contact gaspard.simonet@eurac.edu for questions and remarks !

