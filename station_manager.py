#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Dec  4 12:22:35 2024

@author: gsimonet

Station management and data fetching integration for Netatmo API
"""

import os
import sys
import json
import csv
import requests
from datetime import datetime
from pathlib import Path

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
            response = requests.get(self.api_url, params=params, headers=headers)
            response.raise_for_status()
            return response.text
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data from Netatmo API: {e}")
            return None
    
    def save_station_list(self, json_data, output_dir=None):
        """Parse and save station data to CSV file."""
        try:
            data = json.loads(json_data)
            
            if data.get('status') != 'ok' or 'body' not in data:
                raise ValueError("Invalid data format or status not ok")
            
            # Find maximum number of modules
            max_modules = max(len(station.get('modules', [])) for station in data['body'])
            
            # Extract stations information
            stations = []
            for station in data['body']:
                station_info = {
                    'station_id': station['_id'],
                    'longitude': station['place']['location'][0],
                    'latitude': station['place']['location'][1],
                    'city': station['place'].get('city', 'Unknown'),
                    'country': station['place'].get('country', 'Unknown'),
                    'altitude': station['place'].get('altitude', 'Unknown'),
                    'street': station['place'].get('street', 'Unknown'),
                    'timezone': station['place'].get('timezone', 'Unknown'),
                }
                
                # Add module information
                modules = station.get('modules', [])
                module_types = station.get('module_types', {})
                
                for i in range(max_modules):
                    module_num = i + 1
                    if i < len(modules):
                        module_id = modules[i]
                        module_type = module_types.get(module_id, 'Unknown')
                        station_info[f'module{module_num}_id'] = module_id
                        station_info[f'module{module_num}_type'] = module_type
                    else:
                        station_info[f'module{module_num}_id'] = ''
                        station_info[f'module{module_num}_type'] = ''
                
                stations.append(station_info)
            
            # Prepare CSV headers
            base_fieldnames = ['station_id', 'latitude', 'longitude', 'city', 'country', 
                             'altitude', 'street', 'timezone']
            module_fieldnames = []
            for i in range(max_modules):
                module_num = i + 1
                module_fieldnames.extend([f'module{module_num}_id', f'module{module_num}_type'])
            
            fieldnames = base_fieldnames + module_fieldnames
            
            # Determine output directory and create if necessary
            if output_dir is None:
                output_dir = os.path.join(project_dir, 'station_list')
            os.makedirs(output_dir, exist_ok=True)
            
            # Generate filename with timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_path = os.path.join(output_dir, f'stations_{timestamp}.csv')
            
            # Write to CSV file
            with open(output_path, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(stations)
                
            print(f"Successfully exported {len(stations)} stations to {output_path}")
            return output_path
            
        except Exception as e:
            print(f"Error processing station data: {e}")
            return None

class TemperatureFetcher:
    """Fetches temperature data for stations."""
    
    def __init__(self, access_token):
        self.access_token = access_token
    
    def process_station_list(self, station_list_path):
        """Process temperature data for all stations in the list."""
        try:
            with open(station_list_path, 'r', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                for station in reader:
                    # Your existing station processing logic here
                    print(f"Processing station {station['station_id']}")
                    # Add your temperature fetching logic
        except Exception as e:
            print(f"Error processing station list: {e}")

def main():
    """Main function to run the station discovery and temperature fetching process."""
    # Get tokens
    tokens = get_netatmo_tokens()
    
    if not tokens:
        print("Failed to get Netatmo tokens")
        sys.exit(1)
    
    # Initialize station manager
    station_manager = StationManager(tokens['access_token'])
    
    # Get coordinates from user
    lat_ne, lon_ne, lat_sw, lon_sw = station_manager.get_user_coordinates()
    
    # Fetch station data
    print("\nFetching station data from Netatmo API...")
    json_data = station_manager.fetch_stations(lat_ne, lon_ne, lat_sw, lon_sw)
    
    if json_data:
        # Save station list and get the path
        station_list_path = station_manager.save_station_list(json_data)
        
        if station_list_path:
            # Initialize temperature fetcher
            fetcher = TemperatureFetcher(tokens['access_token'])
            
            # Process all stations
            print("\nProcessing temperature data for all stations...")
            fetcher.process_station_list(station_list_path)
    
if __name__ == "__main__":
    # Add the project directory to Python path
    project_dir = os.path.dirname(os.path.abspath(__file__))
    sys.path.append(project_dir)
    
    from src.auth import get_netatmo_tokens
    
    main()