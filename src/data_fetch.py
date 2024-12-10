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

@dataclass
class FetchConfig:
    API_URL: str = 'https://api.netatmo.com/api/getmeasure'
    DATA_DIR: Path = Path('temperature_station_data')
    CHUNK_SIZE: int = 1024  # Maximum number of hourly measurements per request
    N_DAYS_AGO: int = 465
    REQUEST_DELAY: float = 0  # seconds between requests

class RateLimitManager:
    def __init__(self, requests_per_hour=500):
        self.requests_per_hour = requests_per_hour
        self.counter_file = Path('request_counter.json')
        self.load_counter()

    def load_counter(self):
        """Load or initialize request counter"""
        if self.counter_file.exists():
            with open(self.counter_file) as f:
                data = json.load(f)
                self.request_count = data['count']
                self.hour_start = datetime.fromisoformat(data['hour_start'])
        else:
            self.request_count = 0
            self.hour_start = datetime.now()
            self.save_counter()

    def save_counter(self):
        """Save current counter state"""
        with open(self.counter_file, 'w') as f:
            json.dump({
                'count': self.request_count,
                'hour_start': self.hour_start.isoformat()
            }, f)

    def check_and_update(self):
        """Check if we can make a request and update counter"""
        now = datetime.now()
        
        # Reset counter if an hour has passed
        if now - self.hour_start > timedelta(hours=1):
            self.request_count = 0
            self.hour_start = now
            print("\nNew hour started - Reset request counter")
            self.save_counter()
        
        # Check if we've hit the limit
        if self.request_count >= self.requests_per_hour:
            wait_time = self.hour_start + timedelta(hours=1) - now
            minutes = int(wait_time.total_seconds() / 60)
            print(f"\nRate limit reached. Please wait {minutes} minutes or restart later.")
            print(f"Next reset time: {self.hour_start + timedelta(hours=1)}")
            return False
        
        # Update counter
        self.request_count += 1
        self.save_counter()
        return True

class TemperatureFetcher:
    def __init__(self, access_token: str, config: FetchConfig = FetchConfig()):
        self.access_token = access_token
        self.config = config
        self._ensure_data_dir()
        self.session = self._setup_session()
        self.rate_limiter = RateLimitManager()
    
    def _ensure_data_dir(self):
        """Create data directory if it doesn't exist"""
        self.config.DATA_DIR.mkdir(parents=True, exist_ok=True)

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
        if not self.rate_limiter.check_and_update():
            print("Waiting for rate limit reset...")
            return None
            
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
                return {'body': {}}  # Return empty but valid data structure
                
            return data['body']
            
        except requests.exceptions.RequestException as e:
            print(f"API request failed: {e}")
            return None

    def process_station_list(self, station_list_path: str):
        """Process all stations from CSV file"""
        progress_file = Path('fetch_progress.json')
        
        try:
            # Load station list
            station_list = pd.read_csv(station_list_path)
            total_stations = len(station_list)
            print(f"Found {total_stations} stations to process")
    
            # Find the last timestamp in existing data
            data_dir = Path('temperature_station_data')
            latest_timestamp = None
            if data_dir.exists():
                for file in data_dir.glob('temperature_data_*.csv'):
                    try:
                        df = pd.read_csv(file, index_col=0)
                        df.index = pd.to_datetime(df.index)
                        file_max = df.index.max()
                        if latest_timestamp is None or file_max > latest_timestamp:
                            latest_timestamp = file_max
                    except Exception as e:
                        print(f"Error reading {file}: {e}")
                        continue
            
            if latest_timestamp:
                print(f"Will update data from: {latest_timestamp}")
                date_begin = int(latest_timestamp.timestamp())
            else:
                date_begin = int((datetime.now() - timedelta(days=self.config.N_DAYS_AGO)).timestamp())
    
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
    
                    # Process with latest timestamp
                    df = self.process_temperature_data(
                        i, device_id, module_id, lat, lon, alt,
                        date_begin=date_begin
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
        
        if file_path.exists():
            df = pd.read_csv(file_path, index_col=0)
            df.index = pd.to_datetime(df.index)
        else:
            df = pd.DataFrame(columns=['temperature', 'longitude', 'latitude', 
                                     'altitude', 'station_ID'])
    
        # Use provided date_begin or calculate from config
        if date_begin is None:
            date_begin = self._calculate_start_date()
    
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
                
                all_data.append(new_df)
                print(f"Retrieved {len(new_df)} records")
            
            date_begin = (date_end // 3600 * 3600) + 3600
    
        if all_data:
            # Combine old and new data
            combined_df = pd.concat([df] + all_data)
            # Remove duplicates and sort
            combined_df = combined_df[~combined_df.index.duplicated(keep='last')]
            combined_df = combined_df.sort_index()
            
            # Save and verify
            combined_df.to_csv(file_path)
            print(f"Saved updated data to {file_path}")
            print(f"Total records: {len(combined_df)}")
            
            return combined_df
            
        return df

    def _calculate_start_date(self) -> int:
        """Calculate start date based on N_DAYS_AGO"""
        return (int((datetime.now() - 
                timedelta(days=self.config.N_DAYS_AGO)).timestamp()) 
                // 3600 * 3600)
