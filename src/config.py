# config.py
from dataclasses import dataclass
from typing import Tuple, List
import os
from dotenv import load_dotenv

load_dotenv()

@dataclass
class RadarConfig:
    """Configuration for the radar processing system"""
    # DMI API
    api_url: str = "https://dmigw.govcloud.dk/v1/radardata/collections/composite/items"
    api_key: str = os.getenv('API_KEY', '')
    limit: int = 40
    bbox: str = "7.0,54.0,16.0,58.0"
    
    # Processing
    reflectivity_threshold: int = 70
    image_width: int = 1280
    forecast_steps: int = 6
    forecast_interval_minutes: int = 10
    
    # AWS S3
    aws_access_key: str = os.getenv('AWS_ACCESS_KEY', '')
    aws_secret_key: str = os.getenv('AWS_SECRET_KEY', '')
    bucket_name: str = os.getenv('AWS_BUCKET_NAME', '')
    region_name: str = os.getenv('AWS_REGION', 'eu-west-1')
    endpoint_url: str = os.getenv('AWS_ENDPOINT_URL', 'https://s3.amazonaws.com')
    subfolder: str = os.getenv('SUBFOLDER', 'radar')
    
    # Directories
    h5_folder: str = "h5_files"
    png_folder: str = "png_files"
    forecast_folder: str = "forecast_files"
    
    # Colors for visualization (dBZ values)
    color_scale: List[Tuple[float, float, float, float]] = None
    dbz_values: List[int] = None
    
    def __post_init__(self):
        if self.color_scale is None:
            self.color_scale = [
                (0.0, 1.0, 1.0, 0.8),  # Cyan
                (0.0, 0.0, 1.0, 0.8),  # Blue
                (0.0, 0.0, 0.5, 0.8),  # Dark blue
                (1.0, 1.0, 0.0, 0.8),  # Yellow
                (1.0, 0.65, 0.0, 0.8), # Orange
                (1.0, 0.0, 0.0, 0.8),  # Red
                (0.5, 0.0, 0.5, 0.8)   # Purple
            ]
        if self.dbz_values is None:
            self.dbz_values = [70, 85, 100, 128, 160, 192, 255]