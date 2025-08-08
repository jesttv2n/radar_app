# downloaders/dmi_downloader.py
import os
import requests
import logging
from typing import List, Dict, Any
from tenacity import retry, stop_after_attempt, wait_exponential
from ..config import RadarConfig
from ..exceptions import DownloadError
from ..utils import sanitize_filename, get_current_utc_time

class DMIDownloader:
    """Handles downloading of radar data from DMI API"""
    
    def __init__(self, config: RadarConfig):
        self.config = config
        self.logger = logging.getLogger(__name__)
    
    @retry(stop=stop_after_attempt(3), 
           wait=wait_exponential(multiplier=1, min=4, max=10))
    def fetch_radar_metadata(self) -> Dict[str, Any]:
        """Fetch radar data metadata from DMI API with retry logic"""
        try:
            current_utc_time = get_current_utc_time()
            params = {
                'api-key': self.config.api_key,
                'limit': self.config.limit,
                'datetime': f"../{current_utc_time}",
                'bbox': self.config.bbox
            }
            
            response = requests.get(self.config.api_url, params=params, timeout=30)
            response.raise_for_status()
            
            return response.json()
            
        except requests.RequestException as e:
            self.logger.error(f"Error fetching radar metadata: {e}")
            raise DownloadError(f"Failed to fetch radar metadata: {e}")
    
    def should_skip_file(self, file_datetime: str) -> bool:
        """Determine if file should be skipped based on minute marker"""
        return int(file_datetime[14:16]) % 10 == 5
    
    @retry(stop=stop_after_attempt(3),
           wait=wait_exponential(multiplier=1, min=2, max=8))
    def download_file(self, url: str, output_path: str) -> None:
        """Download a single file with retry logic"""
        try:
            response = requests.get(url, stream=True, timeout=60)
            response.raise_for_status()
            
            with open(output_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            
            self.logger.info(f"Downloaded: {output_path}")
            
        except requests.RequestException as e:
            self.logger.error(f"Error downloading {url}: {e}")
            raise DownloadError(f"Failed to download {url}: {e}")
    
    def download_hdf5_files(self) -> List[str]:
        """Download all HDF5 files and return list of file paths"""
        self.logger.info("Starting HDF5 file download")
        
        # Fetch metadata
        data = self.fetch_radar_metadata()
        
        # Ensure output directory exists
        os.makedirs(self.config.h5_folder, exist_ok=True)
        
        fetched_files = set()
        downloaded_paths = []
        
        for feature in data['features']:
            file_datetime = feature['properties']['datetime']
            
            if self.should_skip_file(file_datetime):
                continue
            
            # Prepare filename
            file_datetime = file_datetime.replace(":", "-")
            file_name = f"{file_datetime}.h5"
            sanitized_file_name = sanitize_filename(file_name)
            output_path = os.path.join(self.config.h5_folder, sanitized_file_name)
            
            fetched_files.add(sanitized_file_name)
            
            # Skip if file already exists
            if os.path.exists(output_path):
                downloaded_paths.append(output_path)
                continue
            
            # Download file
            hdf5_url = feature['asset']['data']['href']
            self.download_file(hdf5_url, output_path)
            downloaded_paths.append(output_path)
        
        # Clean up old files
        self._cleanup_old_files(fetched_files)
        
        self.logger.info(f"Downloaded {len(downloaded_paths)} HDF5 files")
        return downloaded_paths
    
    def _cleanup_old_files(self, current_files: set) -> None:
        """Remove files that are no longer in the current dataset"""
        if not os.path.exists(self.config.h5_folder):
            return
            
        existing_files = set(os.listdir(self.config.h5_folder))
        files_to_delete = existing_files - current_files
        
        for file in files_to_delete:
            try:
                os.remove(os.path.join(self.config.h5_folder, file))
                self.logger.info(f"Deleted old file: {file}")
            except OSError as e:
                self.logger.error(f"Error deleting file {file}: {e}")