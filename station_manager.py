#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Dec  4 12:22:35 2024
@author: gsimonet
"""

import os
import sys
import json
import csv
import requests
from datetime import datetime
from pathlib import Path

# Add src directory to Python path
current_dir = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.join(current_dir, 'src')
if src_dir not in sys.path:
    sys.path.insert(0, src_dir)

print(f"Current directory: {current_dir}")
print(f"Source directory: {src_dir}")

try:
    from auth import get_netatmo_tokens
    print("Successfully imported auth module")
except ImportError as e:
    print(f"Error importing auth module: {e}")
    print(f"Looking for auth.py in: {src_dir}")
    sys.exit(1)

class StationManager:
    """Manages Netatmo weather station discovery and listing."""
    
    def __init__(self, access_token):
        self.access_token = access_token
        self.api_url = "https://api.netatmo.com/api/getpublicdata"
    
    def get_user_coordinates(self):
        """Get coordinate bounds from user input with validation."""
        while True:
            try:
                print("\nPlease enter the coordinates for the area of interest:")
                print("Northeast corner:")
                lat_ne = float(input("Latitude (e.g., 46.51): "))
                lon_ne = float(input("Longitude (e.g., 11.36): "))
                
                print("\nSouthwest corner:")
                lat_sw = float(input("Latitude (e.g., 46.44): "))
                lon_sw = float(input("Longitude (e.g., 11.30): "))
                
                if not (-90 <= lat_ne <= 90 and -90 <= lat_sw <= 90):
                    print("Error: Latitude must be between -90 and 90 degrees")
                    continue
                    
                if not (-180 <= lon_ne <= 180 and -180 <= lon_sw <= 180):
                    print("Error: Longitude must be between -180 and 180 degrees")
                    continue
                    
                if lat_ne <= lat_sw:
                    print("Error: Northeast latitude must be greater than Southwest latitude")
                    continue
                    
                if lon_ne <= lon_sw:
                    print("Error: Northeast longitude must be greater than Southwest longitude")
                    continue
                
                return lat_ne, lon_ne, lat_sw, lon_sw
                
            except ValueError:
                print("Error: Please enter valid numerical coordinates")
    
    def fetch_stations(self, lat_ne, lon_ne, lat_sw, lon_sw):
        """Fetch public station data from Netatmo API."""
        params = {
            "lat_ne": lat_ne,
            "lon_ne": lon_ne,
            "lat_sw": lat_sw,
            "lon_sw": lon_sw,
            "required_data": "temperature",
            "filter": "false"
        }
        
        headers = {
            "accept": "application/json",
            "Authorization": f"Bearer {self.access_token}"
        }
        
        try:
            print("\nSending request to Netatmo API...")
            response = requests.get(self.api_url, params=params, headers=headers)
            response.raise_for_status()
            print("Successfully received response from API")
            return response.text
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data from Netatmo API: {e}")
            if hasattr(e, 'response') and e.response is not None:
                print(f"Response: {e.response.text}")
            return None
    
    def save_station_list(self, json_data, output_dir=None):
        """Parse and save station data to CSV file."""
        try:
            data = json.loads(json_data)
            
            if data.get('status') != 'ok' or 'body' not in data:
                raise ValueError("Invalid data format or status not ok")
            
            # Extract stations information
            stations = []
            for station in data['body']:
                # Basic station info
                station_info = {
                    'station_id': station['_id'],
                    'latitude': station['place']['location'][1],  # Swap to correct order
                    'longitude': station['place']['location'][0],
                    'altitude': station['place'].get('altitude', 'Unknown'),
                    'city': station['place'].get('city', 'Unknown'),
                    'country': station['place'].get('country', 'Unknown'),
                    'timezone': station['place'].get('timezone', 'Unknown'),
                    'street': station['place'].get('street', 'Unknown')
                }
                
                # Handle NAModule1 (outdoor module)
                module_found = False
                module_types = station.get('module_types', {})
                for module_id, module_type in module_types.items():
                    if module_type == "NAModule1":
                        station_info['module1_id'] = module_id
                        station_info['module1_type'] = module_type
                        module_found = True
                        break
                
                if not module_found:
                    station_info['module1_id'] = ''
                    station_info['module1_type'] = ''
                
                stations.append(station_info)
            
            # Determine output directory and create if necessary
            if output_dir is None:
                output_dir = os.path.join(current_dir, 'station_list')
            os.makedirs(output_dir, exist_ok=True)
            
            # Generate filename with timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_path = os.path.join(output_dir, f'stations_{timestamp}.csv')
            
            # Define field names in desired order
            fieldnames = [
                'station_id', 'module1_id', 'module1_type',
                'latitude', 'longitude', 'altitude',
                'city', 'country', 'street', 'timezone'
            ]
            
            # Write to CSV file
            with open(output_path, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames, extrasaction='ignore')
                writer.writeheader()
                writer.writerows(stations)
            
            print(f"\nSuccessfully exported {len(stations)} stations to:")
            print(output_path)
            
            # Print summary of found stations
            print(f"\nFound {len(stations)} total stations")
            stations_with_modules = sum(1 for s in stations if s['module1_id'])
            print(f"Stations with outdoor modules: {stations_with_modules}")
            
            return output_path
            
        except Exception as e:
            print(f"Error processing station data: {e}")
            return None

def main():
    """Main function to run the station discovery process."""
    print("\nStarting Netatmo station discovery...")
    
    # Get tokens
    tokens = get_netatmo_tokens()
    
    if not tokens:
        print("Failed to get Netatmo tokens")
        sys.exit(1)
    
    # Initialize station manager with access token
    station_manager = StationManager(tokens['access_token'])
    
    # Get coordinates from user
    lat_ne, lon_ne, lat_sw, lon_sw = station_manager.get_user_coordinates()
    
    # Fetch station data
    print("\nFetching station data from Netatmo API...")
    json_data = station_manager.fetch_stations(lat_ne, lon_ne, lat_sw, lon_sw)
    
    if json_data:
        # Save station list
        station_list_path = station_manager.save_station_list(json_data)
        
        if station_list_path:
            print("\nStation list has been created successfully!")
            print("You can now use run_fetch.py to download temperature data.")
        else:
            print("\nFailed to save station list")
            sys.exit(1)
    else:
        print("\nFailed to fetch station data")
        sys.exit(1)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nScript stopped by user.")
        sys.exit(0)
    except Exception as e:
        print(f"\nAn error occurred: {e}")
        sys.exit(1)