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
client_id = '****'
client_secret = '****'
initial_access_token = '****'
initial_refresh_token = '****''

def load_tokens():
    """Load tokens from JSON file"""
    try:
        with open(token_file, 'r') as file:
            tokens = json.load(file)
            print(f"Loaded tokens from {token_file}")
            return tokens
    except FileNotFoundError:
        print(f"Note: {token_file} not found, will use initial tokens")
        return None
    except json.JSONDecodeError:
        print(f"Note: Could not decode {token_file}, will use initial tokens")
        return None

def save_tokens(access_token, refresh_token):
    """Save both tokens to JSON file"""
    try:
        with open(token_file, 'w') as file:
            json.dump({
                'access_token': access_token,
                'refresh_token': refresh_token
            }, file, indent=2)
        print(f"Saved tokens to {token_file}")
    except Exception as e:
        print(f"Note: Could not save to {token_file}: {e}")

def refresh_access_token(client_id, client_secret, refresh_token):
    """Get new access token using refresh token"""
    token_url = "https://api.netatmo.com/oauth2/token"
    
    print(f"\nRefreshing access token...")
    print(f"Using client_id: {client_id}")
    print(f"Using refresh_token: {refresh_token[:30]}...")
    
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
        
        # Save the new tokens
        save_tokens(new_access_token, new_refresh_token)
        
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
    
    # Try to load existing tokens
    stored_tokens = load_tokens()
    
    if stored_tokens:
        # Try to use stored tokens
        access_token = stored_tokens.get('access_token')
        refresh_token = stored_tokens.get('refresh_token')
        
        # Try to refresh with stored refresh token
        result = refresh_access_token(client_id, client_secret, refresh_token)
        if result:
            return result
        else:
            print("Failed with stored tokens, trying initial tokens...")
    
    # Use initial tokens
    print("Using initial tokens from configuration...")
    result = refresh_access_token(client_id, client_secret, initial_refresh_token)
    
    if result:
        return result
    else:
        # If refresh fails, return the initial access token
        print("\nRefresh failed, using initial access token...")
        save_tokens(initial_access_token, initial_refresh_token)
        return {
            'access_token': initial_access_token,
            'refresh_token': initial_refresh_token
        }

if __name__ == "__main__":
    print("Netatmo Authentication Test")
    print("-" * 30)
    
    tokens = get_netatmo_tokens()
    if tokens:
        print("\nAuthentication successful!")
        print(f"Access Token: {tokens['access_token'][:30]}...")
        print(f"Refresh Token: {tokens['refresh_token'][:30]}...")
    else:
        print("\nAuthentication failed")
