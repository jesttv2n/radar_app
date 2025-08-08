# utils.py
import re
import requests
import logging
from datetime import datetime, timezone
from typing import Optional
import pytz

def check_internet_connection(timeout: int = 5) -> bool:
    """Check if internet connection is available"""
    try:
        requests.get("http://www.google.com", timeout=timeout)
        return True
    except requests.ConnectionError:
        return False

def sanitize_filename(filename: str) -> str:
    """Remove invalid characters from filename"""
    return re.sub(r'[\\/:*?"<>|]', '', filename)

def get_current_utc_time() -> str:
    """Get current UTC time formatted for API"""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def convert_utc_to_danish(utc_time: datetime) -> datetime:
    """Convert UTC datetime to Danish timezone"""
    utc = pytz.utc
    danish_tz = pytz.timezone('Europe/Copenhagen')
    return utc.localize(utc_time).astimezone(danish_tz)

def extract_timestamp(filename: str) -> Optional[datetime]:
    """Extract timestamp from filename"""
    try:
        timestamp_str = filename.split('.')[0].replace('_forecast_1', '')
        return datetime.strptime(timestamp_str, '%Y-%m-%dT%H-%M-%SZ')
    except ValueError:
        logging.error(f"Error parsing timestamp from filename {filename}")
        return None

def translate_month_to_danish(date_str: str) -> str:
    """Translate English month names to Danish"""
    months_translation = {
        "January": "januar", "February": "februar", "March": "marts",
        "April": "april", "May": "maj", "June": "juni",
        "July": "juli", "August": "august", "September": "september",
        "October": "oktober", "November": "november", "December": "december"
    }
    for eng_month, dan_month in months_translation.items():
        date_str = date_str.replace(eng_month, dan_month)
    return date_str
