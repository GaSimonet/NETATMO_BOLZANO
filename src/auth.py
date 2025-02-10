#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Dec 16 2024
@author: gsimonet
"""

import os
import requests
import json
from pathlib import Path

# Get absolute path to src directory
current_dir = os.path.dirname(os.path.abspath(__file__))
token_file = os.path.join(current_dir, 'tokens.json')

# Authentication details
client_id = '6764472d8312a71ccf005029'
client_secret = 'ezIx4rXqCCCprnKGAA1lxiCH7KIcPB0LmFKBYos7Z'
initial_refresh_token = '6707f09769ac144d3f066c9d|174314a1945d1468e11e4328b5ee94b4'

def load_refresh_token():
    """Load refresh token from JSON file"""
    try:
        with open(token_file, 'r') as file:
            tokens = json.load(file)
            token = tokens.get('refresh_token')
            print(f"Loaded refresh token from {token_file}")
            return token
    except FileNotFoundError:
        print(f"Note: {token_file} not found, will use initial token")
        return None
    except json.JSONDecodeError:
        print(f"Note: Could not decode {token_file}, will use initial token")
        return None

def save_refresh_token(refresh_token):
    """Save refresh token to JSON file"""
    try:
        with open(token_file, 'w') as file:
            json.dump({'refresh_token': refresh_token}, file)
        print(f"Saved new refresh token to {token_file}")
    except Exception as e:
        print(f"Note: Could not save to {token_file}: {e}")

def refresh_access_token(client_id, client_secret, refresh_token):
    """Get new access token using refresh token"""
    token_url = "https://api.netatmo.com/oauth2/token"
    
    print(f"\nRequesting new access token...")
    print(f"Using client_id: {client_id}")
    print(f"Using refresh_token: {refresh_token[:15]}...")
    
    payload = {
        'grant_type': 'refresh_token',
        'refresh_token': refresh_token,
        'client_id': client_id,
        'client_secret': client_secret
    }
    
    response = requests.post(token_url, data=payload)
    if response.status_code == 200:
        new_tokens = response.json()
        new_access_token = new_tokens['access_token']
        new_refresh_token = new_tokens.get('refresh_token', refresh_token)
        print("✓ Access token renewed successfully")
        
        if new_refresh_token != refresh_token:
            print("✓ Got new refresh token, saving...")
            save_refresh_token(new_refresh_token)
        
        return {
            'access_token': new_access_token,
            'refresh_token': new_refresh_token
        }
    else:
        print("\nError renewing access token:")
        print(f"Status code: {response.status_code}")
        print(f"Response: {response.text}")
        return None

def get_netatmo_tokens():
    """Main function to get Netatmo tokens"""
    print("\nCurrent working directory:", os.path.abspath(os.path.dirname(__file__)))
    print(f"Using token file: {token_file}")
    
    # Try to load existing refresh token or use initial one from config
    refresh_token = load_refresh_token() or initial_refresh_token
    
    return refresh_access_token(client_id, client_secret, refresh_token)

if __name__ == "__main__":
    print("Netatmo Authentication Test")
    print("-" * 30)
    
    # First ensure tokens.json exists with initial token
    if not os.path.exists(token_file):
        print(f"\nCreating {token_file} with initial token...")
        save_refresh_token(initial_refresh_token)
    
    tokens = get_netatmo_tokens()
    if tokens:
        print("\nAuthentication successful!")
        print(f"Access Token: {tokens['access_token'][:10]}...")
    else:
        print("\nAuthentication failed")