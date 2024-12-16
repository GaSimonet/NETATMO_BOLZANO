import os
import requests
import pandas as pd
import datetime
import json
from pathlib import Path
from typing import Optional, Dict, Any
from dataclasses import dataclass
from datetime import datetime, timedelta
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import time
import glob

@dataclass
class FetchConfig:
    API_URL: str = 'https://api.netatmo.com/api/getmeasure'
    DATA_DIR: Path = Path('temperature_station_data')
    CHUNK_SIZE: int = 1024  # Maximum number of hourly measurements per request
    REQUEST_DELAY: float = 0  # seconds between requests

class StationDataManager:
    def __init__(self, data_dir: Path):
        self.data_dir = data_dir
        self._ensure_dir_exists()

    def _ensure_dir_exists(self):
        """Create data directory if it doesn't exist"""
        self.data_dir.mkdir(parents=True, exist_ok=True)

    def find_station_file(self, station_id: str) -> Optional[Path]:
        """Find existing file for station"""
        pattern = os.path.join(self.data_dir, f'temperature_data_*.csv')
        for file_path in glob.glob(pattern):
            try:
                df = pd.read_csv(file_path)
                if 'station_ID' in df.columns and station_id in df['station_ID'].values:
                    return Path(file_path)
            except Exception as e:
                print(f"Warning: Error reading {file_path}: {e}")
        return None

    def get_last_timestamp(self, file_path: Path, station_id: str) -> Optional[datetime]:
        """Get last timestamp for specific station from file"""
        try:
            # Read the CSV file with the first column as index
            df = pd.read_csv(file_path, index_col=0, parse_dates=True)
            
            # Filter for the specific station
            station_data = df[df['station_ID'] == station_id]
            
            if not station_data.empty:
                # Convert index to datetime if it isn't already
                if not isinstance(station_data.index, pd.DatetimeIndex):
                    station_data.index = pd.to_datetime(station_data.index)
                
                # Get the maximum timestamp
                last_timestamp = station_data.index.max()
                
                # Verify it's a reasonable timestamp (after 2020)
                if last_timestamp.year < 2020:
                    print(f"Warning: Found suspiciously old timestamp: {last_timestamp}")
                    return None
                
                print(f"Found last timestamp for station {station_id}: {last_timestamp}")
                return last_timestamp
            else:
                print(f"No existing data found for station {station_id}")
                return None
                
        except Exception as e:
            print(f"Warning: Error getting timestamp from {file_path}: {e}")
            return None
class TemperatureFetcher:
    def __init__(self, access_token: str, config: FetchConfig = FetchConfig()):
        self.access_token = access_token
        self.config = config
        self.session = self._setup_session()
        self.data_manager = StationDataManager(config.DATA_DIR)

    def _setup_session(self):
        session = requests.Session()
        retries = Retry(
            total=5,
            backoff_factor=30,
            status_forcelist=[403, 429, 500, 502, 503, 504],
            respect_retry_after_header=True
        )
        session.mount('https://', HTTPAdapter(max_retries=retries))
        return session

    def fetch_temperature_data(self, device_id: str, module_id: str, 
                             date_begin: int, date_end: int) -> Optional[Dict]:
        """Fetch temperature data from the API"""
        try:
            time.sleep(self.config.REQUEST_DELAY)
            
            params = {
                'access_token': self.access_token,
                'device_id': device_id,
                'module_id': module_id,
                'scale': '1hour',
                'type': 'temperature',
                'date_begin': str(date_begin),
                'date_end': str(date_end),
                'optimize': 'false',
                'real_time': 'true'
            }
            
            response = self.session.get(self.config.API_URL, params=params)
            
            if response.status_code == 403:
                print("Rate limit reached, saving progress...")
                return None
            
            response.raise_for_status()
            data = response.json()
            
            if not data.get('body'):
                print(f"No data available for this period - continuing to next time chunk")
                return {'body': {}}
                
            return data['body']
            
        except requests.exceptions.RequestException as e:
            print(f"API request failed: {e}")
            return None

    def process_station_list(self, station_list_path: str, *, start_date: Optional[datetime] = None):
        """Process all stations from CSV file"""
        progress_file = Path('fetch_progress.json')
        
        try:
            # Load station list
            station_list = pd.read_csv(station_list_path)
            total_stations = len(station_list)
            print(f"Found {total_stations} stations to process")

            # Process all stations
            for i in range(total_stations):
                try:
                    print(f"\nProcessing station {i+1}/{total_stations} "
                          f"({((i+1)/total_stations)*100:.1f}%)")
                    
                    device_id = station_list['station_id'][i]
                    module_id = station_list['module1_id'][i]
                    lat = station_list['latitude'][i]
                    lon = station_list['longitude'][i]
                    alt = station_list['altitude'][i]

                    if pd.isna(module_id):
                        print(f"Skipping device {device_id} - no module ID")
                        continue

                    # Look for existing data file for this station
                    existing_file = self.data_manager.find_station_file(device_id)
                    if existing_file:
                        last_timestamp = self.data_manager.get_last_timestamp(existing_file, device_id)
                        if last_timestamp:
                            print(f"Found existing data for station {device_id}, last timestamp: {last_timestamp}")
                            # Use the later of start_date or last_timestamp + 1 hour
                            if start_date:
                                fetch_start = max(start_date, last_timestamp + timedelta(hours=1))
                            else:
                                fetch_start = last_timestamp + timedelta(hours=1)
                        else:
                            fetch_start = start_date
                    else:
                        fetch_start = start_date

                    print(f"Starting fetch from: {fetch_start}")

                    # Process with appropriate start time
                    df = self.process_temperature_data(
                        i, device_id, module_id, lat, lon, alt,
                        date_begin=int(fetch_start.timestamp())
                    )
                    
                    if df is None:  # Rate limit hit
                        with open(progress_file, 'w') as f:
                            json.dump({
                                'last_station': i,
                                'last_timestamp': datetime.now().isoformat(),
                                'message': 'Paused due to rate limit'
                            }, f)
                        return False

                except Exception as e:
                    print(f"Error processing station {i}: {e}")
                    with open(progress_file, 'w') as f:
                        json.dump({
                            'last_station': i-1,
                            'last_timestamp': datetime.now().isoformat(),
                            'error': str(e)
                        }, f)
                    raise

            print("\nAll stations processed successfully!")
            return True

        except Exception as e:
            print(f"Error processing station list: {e}")
            raise

    def process_temperature_data(self, i: int, device_id: str, module_id: str,
                           lat: float, lon: float, alt: float,
                           date_begin: int = None) -> Optional[pd.DataFrame]:
        """Process temperature data for a single station"""
        file_path = self.config.DATA_DIR / f'temperature_data_{i}.csv'
        all_data = []
        
        # Initialize DataFrame with proper columns
        columns = ['temperature', 'longitude', 'latitude', 'altitude', 'station_ID']
        
        if file_path.exists():
            df = pd.read_csv(file_path, index_col=0)
            df.index = pd.to_datetime(df.index)
        else:
            df = pd.DataFrame(columns=columns)
            df.index = pd.DatetimeIndex([])  # Empty datetime index
    
        now_epoch = int(datetime.now().timestamp())
        
        while date_begin < now_epoch:
            date_end = min(date_begin + self.config.CHUNK_SIZE * 3600, now_epoch)
            print(f"Fetching data from {datetime.fromtimestamp(date_begin)} "
                  f"to {datetime.fromtimestamp(date_end)}")
            
            data = self.fetch_temperature_data(device_id, module_id, date_begin, date_end)
            
            if data is None:  # Rate limit hit
                return None
                
            if data:
                new_df = pd.DataFrame.from_dict(data, orient='index', 
                                              columns=['temperature'])
                new_df.index = pd.to_datetime(pd.to_numeric(new_df.index), unit='s')
                
                new_df['longitude'] = lon
                new_df['latitude'] = lat
                new_df['altitude'] = alt
                new_df['station_ID'] = device_id
                
                # Ensure new_df has all required columns
                for col in columns:
                    if col not in new_df.columns:
                        new_df[col] = None
                        
                # Only append if we have actual data
                if not new_df.empty:
                    all_data.append(new_df)
                    print(f"Retrieved {len(new_df)} records")
            
            date_begin = (date_end // 3600 * 3600) + 3600
    
        if all_data:
            # Only proceed with concatenation if we have new data
            if len(all_data) > 0:
                # Combine with existing data only if it exists and has actual data
                if not df.empty:
                    all_data.insert(0, df)
                
                # Concatenate all non-empty DataFrames
                combined_df = pd.concat(all_data, axis=0)
                
                # Remove duplicates and sort
                combined_df = combined_df[~combined_df.index.duplicated(keep='last')]
                combined_df = combined_df.sort_index()
                
                # Ensure all columns are present
                for col in columns:
                    if col not in combined_df.columns:
                        combined_df[col] = None
                
                # Reorder columns to match expected structure
                combined_df = combined_df[columns]
                
                # Save and verify
                combined_df.to_csv(file_path)
                print(f"Saved updated data to {file_path}")
                print(f"Total records: {len(combined_df)}")
                
                return combined_df
        
        # If we have no new data but existing data exists, return it
        if not df.empty:
            return df
            
        # Create an empty DataFrame with proper structure if we have no data at all
        empty_df = pd.DataFrame(columns=columns)
        empty_df.index = pd.DatetimeIndex([])
        return empty_df