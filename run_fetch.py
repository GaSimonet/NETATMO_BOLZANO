#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Dec  2 17:54:57 2024
@author: gsimonet
"""
import os
import sys
import json
from glob import glob
from datetime import datetime, timedelta
import time
from pathlib import Path

# Add the project directory to Python path
project_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(project_dir)

from src.auth import get_netatmo_tokens
from src.data_fetch import TemperatureFetcher, FetchConfig

def list_available_station_files(station_dir):
    """List available station files."""
    station_files = glob(os.path.join(station_dir, 'stations_*.csv'))
    
    if not station_files:
        print("No station lists found in directory.")
        return None
        
    print("\nAvailable station lists:")
    for i, filepath in enumerate(station_files, 1):
        filename = os.path.basename(filepath)
        print(f"{i}. {filename}")
    
    while True:
        try:
            choice = int(input("\nSelect a station list (enter number): "))
            if 1 <= choice <= len(station_files):
                return station_files[choice - 1]
            print("Invalid choice. Please try again.")
        except ValueError:
            print("Please enter a valid number.")
1
def try_with_new_token(station_list_path):
    """Try to continue processing with a new token"""
    print("\nAttempting to continue with a fresh token...")
    tokens = get_netatmo_tokens()
    if tokens:
        config = FetchConfig()
        fetcher = TemperatureFetcher(tokens['access_token'], config)
        result = fetcher.process_station_list(station_list_path)
        return result
    return False

def run_fetcher():
    # Get initial station list path
    station_dir = os.path.join(project_dir, 'station_list')
    station_list_path = list_available_station_files(station_dir)
    
    if not station_list_path:
        print("No station list available. Please create a station list first.")
        return

    print(f"\nSelected station list: {os.path.basename(station_list_path)}")

    # Check for existing progress
    progress_file = Path('fetch_progress.json')
    if progress_file.exists():
        with open(progress_file) as f:
            progress = json.load(f)
            print("\nFound previous progress:")
            print(f"Last processed station: {progress['last_station'] + 1}")
            print(f"Last run time: {progress['last_timestamp']}")
            if 'message' in progress:
                print(f"Status: {progress['message']}")

    while True:
        tokens = get_netatmo_tokens()
        if not tokens:
            print("Failed to get authentication tokens")
            return
        
        config = FetchConfig()
        fetcher = TemperatureFetcher(tokens['access_token'], config)
        
        print(f"\nProcessing stations from: {os.path.basename(station_list_path)}")
        result = fetcher.process_station_list(station_list_path)
        
        if not result:
            # First try with a new token
            new_result = try_with_new_token(station_list_path)
            if new_result:
                continue
            
            # If new token didn't help, wait an hour
            next_run = datetime.now() + timedelta(hours=1)
            print("\nProcessing paused - even new token didn't help!")
            print("--------------------")
            print(f"Automatically retrying in one hour (at {next_run.strftime('%H:%M:%S')})")
            print("You can safely stop the script with Ctrl+C if needed")
            print("Progress is saved and you can continue later")
            time.sleep(3600)
            continue
        else:
            print("\nAll stations processed successfully!")
            break

if __name__ == "__main__":
    try:
        run_fetcher()
    except KeyboardInterrupt:
        print("\nScript stopped by user. Progress has been saved.")
        print("You can run the script later to continue from the last processed station.")