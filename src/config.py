from dataclasses import dataclass
from pathlib import Path
from typing import Tuple

@dataclass
class AuthConfig:
    CLIENT_ID: str
    CLIENT_SECRET: str
    TOKEN_FILE: Path = Path('tokens.json')
    TOKEN_URL: str = "https://api.netatmo.com/oauth2/token"

@dataclass
class FetchConfig:
    API_URL: str = 'https://api.netatmo.com/api/getmeasure'
    DATA_DIR: Path = Path('temperature_station_data')
    CHUNK_SIZE: int = 1024 * 3600
    N_DAYS_AGO: int = 465
    TEMPERATURE_RANGE: Tuple[float, float] = (-40, 60)
    REQUEST_DELAY: float = 1.0

@dataclass
class LogConfig:
    LEVEL: str = 'INFO'
    FORMAT: str = '%(asctime)s - %(levelname)s - %(message)s'
    FILE: Path = Path('temperature_fetch.log')

