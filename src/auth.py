import json
import requests
from pathlib import Path
from typing import Optional, Dict
from datetime import datetime
from dataclasses import dataclass

@dataclass
class AuthConfig:
    # Your credentials here
    CLIENT_ID: str = '***********'  # Replace with your client ID
    CLIENT_SECRET: str = '************'  # Replace with your client secret
    TOKEN_FILE: Path = Path('tokens.json')
    TOKEN_URL: str = "https://api.netatmo.com/oauth2/token"
    # Your initial refresh token here
    INITIAL_REFRESH_TOKEN: str = '************'  # Replace with your refresh token

class NetatmoAuthenticator:
    def __init__(self, config: AuthConfig = AuthConfig()):
        self.config = config
        self._validate_config()
    
    def _validate_config(self):
        """Validate that required credentials are set"""
        if not self.config.CLIENT_ID or self.config.CLIENT_ID == 'your_client_id':
            raise ValueError("Please set a valid CLIENT_ID")
        if not self.config.CLIENT_SECRET or self.config.CLIENT_SECRET == 'your_client_secret':
            raise ValueError("Please set a valid CLIENT_SECRET")

    def load_refresh_token(self) -> Optional[str]:
        """Load refresh token from JSON file"""
        try:
            with open(self.config.TOKEN_FILE, 'r') as file:
                tokens = json.load(file)
                return tokens.get('refresh_token')
        except FileNotFoundError:
            print(f"Token file not found at {self.config.TOKEN_FILE}")
            return None
        except json.JSONDecodeError:
            print(f"Error decoding token file at {self.config.TOKEN_FILE}")
            return None

    def save_refresh_token(self, refresh_token: str):
        """Save refresh token to JSON file"""
        try:
            with open(self.config.TOKEN_FILE, 'w') as file:
                json.dump({
                    'refresh_token': refresh_token,
                    'updated_at': datetime.now().isoformat()
                }, file, indent=2)
            print(f"Refresh token saved to {self.config.TOKEN_FILE}")
        except Exception as e:
            print(f"Error saving refresh token: {str(e)}")

    def refresh_access_token(self, refresh_token: Optional[str] = None) -> Optional[Dict[str, str]]:
        """Get new access token using refresh token"""
        # Use provided refresh token or initial refresh token from config
        refresh_token = refresh_token or self.config.INITIAL_REFRESH_TOKEN
        
        payload = {
            'grant_type': 'refresh_token',
            'refresh_token': refresh_token,
            'client_id': self.config.CLIENT_ID,
            'client_secret': self.config.CLIENT_SECRET
        }
        
        try:
            response = requests.post(self.config.TOKEN_URL, data=payload)
            response.raise_for_status()
            
            tokens = response.json()
            new_access_token = tokens['access_token']
            new_refresh_token = tokens.get('refresh_token', refresh_token)
            
            print("Access token renewed successfully")
            
            if new_refresh_token != refresh_token:
                print("New refresh token received")
                self.save_refresh_token(new_refresh_token)
            
            return {
                'access_token': new_access_token,
                'refresh_token': new_refresh_token
            }
            
        except requests.exceptions.RequestException as e:
            print(f"Error refreshing access token: {str(e)}")
            if hasattr(e.response, 'text'):
                print(f"Response: {e.response.text}")
            return None

def get_netatmo_tokens() -> Optional[Dict[str, str]]:
    """Main function to get Netatmo tokens"""
    try:
        auth = NetatmoAuthenticator()
        
        # Try to load existing refresh token or use initial one from config
        refresh_token = auth.load_refresh_token() or auth.config.INITIAL_REFRESH_TOKEN
        
        return auth.refresh_access_token(refresh_token)
        
    except Exception as e:
        print(f"Authentication error: {str(e)}")
        return None

# Example usage 
if __name__ == "__main__":
    tokens = get_netatmo_tokens()
    if tokens:
        print("Authentication successful!")
        print(f"Access token: {tokens['access_token'][:10]}...")  # Show first 10 chars
    else:
        print("Authentication failed")
