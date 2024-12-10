import unittest
from unittest.mock import patch, MagicMock, mock_open
import json
import pandas as pd
import numpy as np
import xarray as xr
from pathlib import Path
from datetime import datetime, timedelta
import os
import sys
import logging
from tests.test_logger import setup_logger

# Setup logger
logger = setup_logger()

# Add project root to Python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from src.auth import NetatmoAuthenticator, AuthConfig, get_netatmo_tokens
from src.data_fetch import TemperatureFetcher, FetchConfig, RateLimitManager
from src.data_manager import DataManager

class TestBase(unittest.TestCase):
    """Base test class with logging setup"""
    
    @classmethod
    def setUpClass(cls):
        """Set up test class"""
        logger.info(f"Starting test class: {cls.__name__}")
        
    def setUp(self):
        """Set up test case"""
        logger.info(f"Starting test: {self._testMethodName}")
        
    def tearDown(self):
        """Clean up after test case"""
        logger.info(f"Finished test: {self._testMethodName}")
        
    @classmethod
    def tearDownClass(cls):
        """Clean up after test class"""
        logger.info(f"Finished test class: {cls.__name__}")

class TestAuthConfig(TestBase):
    """Tests for authentication configuration"""
    
    def setUp(self):
        super().setUp()
        logger.info("Setting up AuthConfig test")
        self.test_config = AuthConfig(
            CLIENT_ID='test_id',
            CLIENT_SECRET='test_secret',
            TOKEN_FILE=Path('test_tokens.json'),
            INITIAL_REFRESH_TOKEN='test_refresh_token'
        )
        logger.debug(f"Test config created: {vars(self.test_config)}")

    def test_auth_config_validation(self):
        """Test configuration validation"""
        logger.info("Testing auth config validation")
        try:
            config = AuthConfig(CLIENT_ID='your_client_id', 
                              CLIENT_SECRET='your_client_secret')
            auth = NetatmoAuthenticator(config)
            self.fail("Should have raised ValueError")
        except ValueError as e:
            logger.info("Successfully caught invalid config")
            logger.debug(f"Error message: {str(e)}")

class TestNetatmoAuthenticator(TestBase):
    """Tests for NetatmoAuthenticator"""
    
    def setUp(self):
        super().setUp()
        logger.info("Setting up NetatmoAuthenticator test")
        self.test_config = AuthConfig(
            CLIENT_ID='test_id',
            CLIENT_SECRET='test_secret',
            TOKEN_FILE=Path('test_tokens.json'),
            INITIAL_REFRESH_TOKEN='test_refresh_token'
        )
        self.auth = NetatmoAuthenticator(self.test_config)

    def test_refresh_access_token(self):
        """Test token refresh"""
        logger.info("Testing token refresh")
        mock_response = {
            'access_token': 'new_access_token',
            'refresh_token': 'new_refresh_token'
        }
        
        with patch('requests.post') as mock_post:
            logger.debug("Setting up mock response")
            mock_post.return_value.json.return_value = mock_response
            mock_post.return_value.status_code = 200
            
            logger.info("Attempting token refresh")
            result = self.auth.refresh_access_token('test_refresh_token')
            
            logger.debug(f"Token refresh result: {result}")
            self.assertEqual(result['access_token'], 'new_access_token')
            logger.info("Token refresh test successful")

class TestTemperatureFetcher(TestBase):
    """Tests for TemperatureFetcher"""
    
    def setUp(self):
        super().setUp()
        logger.info("Setting up TemperatureFetcher test")
        self.config = FetchConfig(
            DATA_DIR=Path('test_data'),
            CHUNK_SIZE=24,
            N_DAYS_AGO=7
        )
        self.fetcher = TemperatureFetcher('test_token', self.config)
        logger.debug(f"Fetcher config: {vars(self.config)}")

    def test_fetch_temperature_data(self):
        """Test temperature data fetching"""
        logger.info("Testing temperature data fetch")
        mock_response = {
            'body': {
                '1609459200': [15.5],
                '1609462800': [16.0]
            }
        }
        
        with patch('requests.Session.get') as mock_get:
            logger.debug("Setting up mock response")
            mock_get.return_value.json.return_value = mock_response
            mock_get.return_value.status_code = 200
            
            logger.info("Attempting to fetch temperature data")
            result = self.fetcher.fetch_temperature_data(
                'device1', 'module1', 1609459200, 1609462800
            )
            
            logger.debug(f"Fetch result: {result}")
            self.assertEqual(result, mock_response['body'])
            logger.info("Temperature fetch test successful")

class TestDataManager(TestBase):
    """Tests for DataManager"""
    
    def setUp(self):
        super().setUp()
        logger.info("Setting up DataManager test")
        self.data_manager = DataManager()
        self.test_data = pd.DataFrame({
            'temperature': [20.5, 21.0],
            'timestamp': ['2024-01-01', '2024-01-02']
        })
        logger.debug(f"Test data created: {self.test_data.head()}")

    def test_save_station_data(self):
        """Test saving station data"""
        logger.info("Testing station data save")
        try:
            self.data_manager.save_station_data(self.test_data, 'station1')
            file_path = self.data_manager.raw_dir / 'station_station1.csv'
            self.assertTrue(file_path.exists())
            logger.info("Successfully saved station data")
            logger.debug(f"Data saved to: {file_path}")
        except Exception as e:
            logger.error(f"Failed to save station data: {str(e)}")
            raise

    def tearDown(self):
        """Clean up test files"""
        super().tearDown()
        logger.info("Cleaning up test files")
        try:
            for path in [self.data_manager.raw_dir, 
                        self.data_manager.processed_dir, 
                        self.data_manager.qc_dir]:
                if path.exists():
                    for file in path.glob('*'):
                        file.unlink()
                    path.rmdir()
            logger.info("Cleanup successful")
        except Exception as e:
            logger.error(f"Cleanup failed: {str(e)}")

if __name__ == '__main__':
    logger.info("Starting test suite")
    unittest.main()