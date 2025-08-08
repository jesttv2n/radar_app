# forecasters/fluid_dynamics_forecaster.py
import numpy as np
import cv2
import logging
from typing import List, Tuple, Optional
from datetime import datetime, timedelta
from scipy.ndimage import gaussian_filter
from ..config import RadarConfig
from ..exceptions import ForecastError
from ..models import RadarFrame, ForecastResult

class FluidDynamicsForecaster:
    """Advanced weather forecasting using fluid dynamics principles"""
    
    def __init__(self, config: RadarConfig):
        self.config = config
        self.logger = logging.getLogger(__name__)
    
    def calculate_optical_flow(self, frame1: np.ndarray, frame2: np.ndarray) -> Tuple[np.ndarray, np.ndarray]:
        """Calculate optical flow between two frames using Lucas-Kanade method"""
        try:
            # Convert to 8-bit for OpenCV
            frame1_8bit = np.clip(frame1, 0, 255).astype(np.uint8)
            frame2_8bit = np.clip(frame2, 0, 255).astype(np.uint8)
            
            # Calculate dense optical flow
            flow = cv2.calcOpticalFlowPyrLK(
                frame1_8bit, frame2_8bit,
                np.float32([[x, y] for y in range(0, frame1.shape[0], 10) 
                           for x in range(0, frame1.shape[1], 10)]).reshape(-1, 1, 2),
                None
            )[0]
            
            # Create velocity field
            u = np.zeros_like(frame1, dtype=np.float32)
            v = np.zeros_like(frame1, dtype=np.float32)
            
            if flow is not None:
                for i, (x, y) in enumerate(flow.reshape(-1, 2)):
                    orig_y, orig_x = divmod(i, frame1.shape[1] // 10)
                    orig_y *= 10
                    orig_x *= 10
                    if 0 <= orig_y < frame1.shape[0] and 0 <= orig_x < frame1.shape[1]:
                        u[orig_y, orig_x] = x - orig_x
                        v[orig_y, orig_x] = y - orig_y
            
            # Smooth velocity field
            u = gaussian_filter(u, sigma=2.0)
            v = gaussian_filter(v, sigma=2.0)
            
            return u, v
            
        except Exception as e:
            self.logger.error(f"Error calculating optical flow: {e}")
            raise ForecastError(f"Optical flow calculation failed: {e}")
    
    def advect_field(self, field: np.ndarray, u: np.ndarray, v: np.ndarray, 
                     dt: float) -> np.ndarray:
        """Advect scalar field using velocity field (semi-Lagrangian scheme)"""
        try:
            rows, cols = field.shape
            
            # Create coordinate grids
            y_coords, x_coords = np.mgrid[0:rows, 0:cols].astype(np.float32)
            
            # Calculate backward trajectories
            x_back = x_coords - u * dt
            y_back = y_coords - v * dt
            
            # Clip to valid range
            x_back = np.clip(x_back, 0, cols - 1)
            y_back = np.clip(y_back, 0, rows - 1)
            
            # Interpolate using bilinear interpolation
            advected = cv2.remap(field.astype(np.float32), x_back, y_back, 
                               cv2.INTER_LINEAR, borderMode=cv2.BORDER_CONSTANT)
            
            return advected
            
        except Exception as e:
            self.logger.error(f"Error in field advection: {e}")
            raise ForecastError(f"Field advection failed: {e}")
    
    def apply_precipitation_decay(self, field: np.ndarray, dt: float, 
                                 decay_rate: float = 0.1) -> np.ndarray:
        """Apply exponential decay to precipitation intensity"""
        decay_factor = np.exp(-decay_rate * dt / 3600.0)  # dt in seconds
        return field * decay_factor
    
    def apply_growth_decay_model(self, field: np.ndarray, dt: float) -> np.ndarray:
        """Apply simplified growth/decay based on intensity"""
        try:
            # Growth for moderate intensities, decay for very high intensities
            growth_mask = (field > 80) & (field < 150)
            decay_mask = field > 180
            
            result = field.copy()
            
            # Slight growth for moderate precipitation
            result[growth_mask] *= (1.0 + 0.05 * dt / 3600.0)
            
            # Stronger decay for intense precipitation
            result[decay_mask] *= (1.0 - 0.15 * dt / 3600.0)
            
            # General precipitation decay
            result = self.apply_precipitation_decay(result, dt)
            
            # Ensure values stay within valid range
            result = np.clip(result, 0, 255)
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error in growth/decay model: {e}")
            return field
    
    def calculate_average_velocity(self, frames: List[RadarFrame]) -> Tuple[np.ndarray, np.ndarray]:
        """Calculate average velocity field from multiple frame pairs"""
        if len(frames) < 2:
            raise ForecastError("Need at least 2 frames to calculate velocity")
        
        u_sum = None
        v_sum = None
        valid_pairs = 0
        
        for i in range(len(frames) - 1):
            try:
                u, v = self.calculate_optical_flow(frames[i].data, frames[i + 1].data)
                
                if u_sum is None:
                    u_sum = u.copy()
                    v_sum = v.copy()
                else:
                    u_sum += u
                    v_sum += v
                    
                valid_pairs += 1
                
            except Exception as e:
                self.logger.warning(f"Skipping frame pair {i}: {e}")
                continue
        
        if valid_pairs == 0:
            raise ForecastError("No valid frame pairs for velocity calculation")
        
        # Return average velocity
        return u_sum / valid_pairs, v_sum / valid_pairs
    
    def generate_forecast(self, frames: List[RadarFrame]) -> List[ForecastResult]:
        """Generate forecast using fluid dynamics approach"""
        self.logger.info("Starting fluid dynamics forecast generation")
        
        if len(frames) < 3:
            raise ForecastError("Need at least 3 frames for forecasting")
        
        try:
            # Use latest 10 frames for velocity calculation
            recent_frames = frames[-min(10, len(frames)):]
            
            # Calculate average velocity field
            u_avg, v_avg = self.calculate_average_velocity(recent_frames)
            
            # Start with the most recent frame
            current_field = recent_frames[-1].data.copy().astype(np.float32)
            current_time = recent_frames[-1].timestamp
            
            forecasts = []
            dt = self.config.forecast_interval_minutes * 60  # Convert to seconds
            
            for step in range(1, self.config.forecast_steps + 1):
                # Advect the field forward in time
                current_field = self.advect_field(current_field, u_avg, v_avg, dt)
                
                # Apply growth/decay model
                current_field = self.apply_growth_decay_model(current_field, dt)
                
                # Calculate confidence based on distance from last observation
                confidence = max(0.1, 1.0 - (step * 0.15))  # Decrease with time
                
                # Create forecast result
                forecast_time = current_time + timedelta(minutes=step * self.config.forecast_interval_minutes)
                
                forecast_result = ForecastResult(
                    timestamp=forecast_time,
                    data=current_field.astype(np.uint8),
                    confidence=confidence,
                    method="fluid_dynamics"
                )
                
                forecasts.append(forecast_result)
                
                self.logger.info(f"Generated forecast step {step}/{self.config.forecast_steps}")
            
            self.logger.info(f"Successfully generated {len(forecasts)} forecast steps")
            return forecasts
            
        except Exception as e:
            self.logger.error(f"Error in forecast generation: {e}")
            raise ForecastError(f"Forecast generation failed: {e}")