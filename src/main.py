# main.py
import time
import logging
import sys
from typing import List
from config import RadarConfig
from downloaders.dmi_downloader import DMIDownloader
from processors.radar_processor import RadarProcessor
from forecasters.fluid_dynamics_forecaster import FluidDynamicsForecaster
from uploaders.s3_uploader import S3Uploader
from exceptions import RadarProcessingError
from utils import check_internet_connection
from models import RadarFrame, ForecastResult

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('radar_worker.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

class RadarWorker:
    """Main orchestrator for the radar processing pipeline"""
    
    def __init__(self):
        self.config = RadarConfig()
        self.logger = logging.getLogger(__name__)
        
        # Initialize components
        self.downloader = DMIDownloader(self.config)
        self.processor = RadarProcessor(self.config)
        self.forecaster = FluidDynamicsForecaster(self.config)
        self.uploader = S3Uploader(self.config)
        
        self.logger.info("RadarWorker initialized")
    
    def validate_config(self) -> bool:
        """Validate configuration before starting"""
        required_fields = ['api_key', 'aws_access_key', 'aws_secret_key', 'bucket_name']
        
        for field in required_fields:
            if not getattr(self.config, field):
                self.logger.error(f"Missing required configuration: {field}")
                return False
        
        return True
    
    def process_cycle(self) -> None:
        """Execute one complete processing cycle"""
        try:
            self.logger.info("Starting processing cycle")
            
            # Step 1: Download HDF5 files
            hdf5_files = self.downloader.download_hdf5_files()
            if not hdf5_files:
                self.logger.warning("No HDF5 files downloaded")
                return
            
            # Step 2: Process to PNG
            png_files = self.processor.batch_process(hdf5_files)
            self.logger.info(f"Processed {len(png_files)} PNG files")
            
            # Step 3: Load radar frames for forecasting
            radar_frames = []
            for hdf5_file in sorted(hdf5_files)[-10:]:  # Use latest 10 frames
                try:
                    frame = self.processor.load_hdf5_data(hdf5_file)
                    radar_frames.append(frame)
                except Exception as e:
                    self.logger.error(f"Error loading frame {hdf5_file}: {e}")
                    continue
            
            # Step 4: Generate forecasts
            forecast_png_files = []
            if len(radar_frames) >= 3:
                try:
                    forecasts = self.forecaster.generate_forecast(radar_frames)
                    
                    # Convert forecasts to PNG
                    for forecast in forecasts:
                        # Create temporary RadarFrame for processing
                        forecast_frame = RadarFrame(
                            timestamp=forecast.timestamp,
                            data=forecast.data,
                            file_path=""
                        )
                        
                        png_path = self.processor.process_to_png(forecast_frame, is_forecast=True)
                        forecast_png_files.append(png_path)
                    
                    self.logger.info(f"Generated {len(forecast_png_files)} forecast images")
                    
                except Exception as e:
                    self.logger.error(f"Forecast generation failed: {e}")
            
            # Step 5: Upload to S3
            try:
                self.uploader.upload_current_images(self.config.png_folder)
                if forecast_png_files:
                    self.uploader.upload_forecast_images(self.config.forecast_folder)
                self.logger.info("Upload completed successfully")
            except Exception as e:
                self.logger.error(f"Upload failed: {e}")
            
            self.logger.info("Processing cycle completed successfully")
            
        except RadarProcessingError as e:
            self.logger.error(f"Processing error: {e}")
        except Exception as e:
            self.logger.error(f"Unexpected error in processing cycle: {e}")
    
    def run(self) -> None:
        """Main execution loop"""
        if not self.validate_config():
            self.logger.error("Configuration validation failed")
            sys.exit(1)
        
        self.logger.info("Starting RadarWorker main loop")
        
        while True:
            try:
                if not check_internet_connection():
                    self.logger.warning("No internet connection, skipping cycle")
                    time.sleep(60)
                    continue
                
                self.process_cycle()
                
                # Sleep for 5 minutes
                self.logger.info("Waiting 5 minutes before next cycle...")
                time.sleep(300)
                
            except KeyboardInterrupt:
                self.logger.info("Received interrupt signal, shutting down...")
                break
            except Exception as e:
                self.logger.error(f"Unexpected error in main loop: {e}")
                time.sleep(60)  # Wait before retrying

def main():
    """Entry point"""
    worker = RadarWorker()
    worker.run()

if __name__ == "__main__":
    main()