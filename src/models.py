# models.py
from dataclasses import dataclass
from datetime import datetime
from typing import Optional
import numpy as np

@dataclass
class RadarFrame:
    """Represents a single radar data frame"""
    timestamp: datetime
    data: np.ndarray
    file_path: str
    processed: bool = False
    
@dataclass 
class ForecastResult:
    """Represents forecast output"""
    timestamp: datetime
    data: np.ndarray
    confidence: float
    method: str

# exceptions.py
class RadarProcessingError(Exception):
    """Base exception for radar processing errors"""
    pass

class DownloadError(RadarProcessingError):
    """Raised when download fails"""
    pass

class ProcessingError(RadarProcessingError):
    """Raised when image processing fails"""
    pass

class ForecastError(RadarProcessingError):
    """Raised when forecasting fails"""
    pass

class UploadError(RadarProcessingError):
    """Raised when upload fails"""
    pass