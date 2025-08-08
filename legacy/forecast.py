import h5py
import numpy as np
import os
import logging
import cv2
from PIL import Image, ImageDraw, ImageFont
from datetime import datetime, timedelta
import pytz
import matplotlib.pyplot as plt
from matplotlib.colors import Normalize, ListedColormap

# Configure logging
logging.basicConfig(
    filename='forecast.log',
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Define constants
h5_output_folder = "h5_files"
forecast_folder = "forecast_files"
REFLECTIVITY_THRESHOLD = 0

# Function to add timestamp to an image
def add_timestamp(image, timestamp, text="Prognose"):
    draw = ImageDraw.Draw(image)
    font_size = 20
    try:
        font = ImageFont.truetype("arial.ttf", font_size)
    except IOError:
        font = ImageFont.load_default()
    timestamp_text = timestamp.strftime('%d. %B %Y - %H:%M')
    text_bbox = draw.textbbox((0, 0), timestamp_text, font=font)
    text_width = text_bbox[2] - text_bbox[0]
    text_height = text_bbox[3] - text_bbox[1]
    width, height = image.size
    x = width - text_width - 10
    y = 10
    draw.text((x, y), timestamp_text, font=font, fill="black")
    draw.text((x, y + text_height + 5), text, font=font, fill="black")
    return image

# Function to convert UTC to Danish time
def convert_utc_to_danish(utc_time):
    utc = pytz.utc
    danish_tz = pytz.timezone('Europe/Copenhagen')
    utc_dt = utc.localize(utc_time)
    danish_dt = utc_dt.astimezone(danish_tz)
    return danish_dt

# Create a custom color map for 8-bit values (0-255)
colors = [
    (0.9, 0.9, 0.9, 0.0),  # Transparent for values below threshold
    (0.0, 1.0, 1.0),  # Cyan for low values
    (0.0, 0.0, 1.0),  # Blue for moderate values
    (0.0, 0.0, 0.5),  # Dark blue for higher values
    (1.0, 1.0, 0.0),  # Yellow for even higher values
    (1.0, 0.65, 0.0),  # Orange for very high values
    (1.0, 0.0, 0.0),  # Red for extremely high values
    (0.5, 0.0, 0.5)   # Purple for the highest values
]
# Define the range for 8-bit values
dbz_values = [0, 32, 64, 96, 128, 160, 192, 224, 255]
cmap = ListedColormap(colors)
norm = Normalize(vmin=min(dbz_values), vmax=max(dbz_values))

# Function to perform linear interpolation
def interpolate_data(data1, data2, alpha):
    return (1 - alpha) * data1 + alpha * data2

# Generate forecast images based on the latest HDF5 files
def generate_forecast(hdf5_files, forecast_folder):
    logging.debug("Starting generate_forecast function")
    if not os.path.exists(forecast_folder):
        os.makedirs(forecast_folder)

    # Read the latest 10 HDF5 files
    hdf5_files.sort()
    latest_files = hdf5_files[-10:]
    datasets = []

    for file_path in latest_files:
        try:
            with h5py.File(file_path, 'r') as file:
                data = file['dataset1']['data1']['data'][:]
                data[data == 255] = 0  # Replace 255 values with 0
                data[data < REFLECTIVITY_THRESHOLD] = 0
                datasets.append(data)
                logging.debug(f"Read data from {file_path} with shape {data.shape}")
        except Exception as e:
            logging.error(f"Error reading HDF5 file {file_path}: {e}")

    if len(datasets) < 2:
        logging.error("Not enough data files to generate forecast.")
        return

    # Use the timestamp from the latest HDF5 file
    latest_file_timestamp = datetime.strptime(os.path.basename(latest_files[-1]), '%Y-%m-%dT%H-%M-%SZ.h5')

    # Generate forecast images
    existing_forecast_files = {os.path.splitext(f)[0] for f in os.listdir(forecast_folder) if f.endswith('.png')}
    new_forecast_files = set()

    for i in range(6):  # Generate 6 forecast images
        alpha = (i + 1) / 6
        forecast_data = interpolate_data(datasets[-2], datasets[-1], alpha)
        logging.debug(f"Interpolated data for forecast step {i+1} with alpha {alpha}")

        # Explicitly set all 255 values to 0 to ensure transparency
        forecast_data[forecast_data == 255] = 0

        # Create a normalized color map with custom colors
        mapped_data = cmap(norm(forecast_data))

        rgba_data = (mapped_data * 255).astype(np.uint8)
        rgba_data[..., 3] = np.where(forecast_data == 0, 0, 255)

        forecast_image = Image.fromarray(rgba_data, 'RGBA')

        forecast_time = convert_utc_to_danish(latest_file_timestamp + timedelta(minutes=10 * (i + 1)))
        forecast_image = add_timestamp(forecast_image, forecast_time)

        file_name = forecast_time.strftime('%Y-%m-%dT%H-%M-%SZ')
        new_forecast_files.add(file_name)

        png_path = os.path.join(forecast_folder, file_name + ".png")
        forecast_image.save(png_path)
        print(f"Saved forecast PNG file: {png_path}")
        logging.debug(f"Saved forecast PNG file: {png_path}")

    # Delete forecast PNG files that do not match the generated ones
    for forecast_file in existing_forecast_files:
        if forecast_file not in new_forecast_files:
            try:
                os.remove(os.path.join(forecast_folder, forecast_file + ".png"))
                print(f"Deleted forecast PNG file: {forecast_file}.png")
                logging.debug(f"Deleted forecast PNG file: {forecast_file}.png")
            except Exception as e:
                logging.error(f"Error deleting file: {e}")
                print(f"Error deleting file: {e}")

def main():
    logging.debug("Starting main function")
    hdf5_files = [os.path.join(h5_output_folder, file) for file in os.listdir(h5_output_folder) if file.endswith('.h5')]
    if hdf5_files:
        logging.debug(f"Found HDF5 files: {hdf5_files}")
        generate_forecast(hdf5_files, forecast_folder)
    else:
        print("No HDF5 files found to generate forecast.")
        logging.debug("No HDF5 files found to generate forecast.")

if __name__ == "__main__":
    main()
