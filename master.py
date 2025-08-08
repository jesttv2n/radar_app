import os
import re
import requests
import logging
import h5py
import numpy as np
import boto3
import time
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
from PIL import Image, ImageDraw, ImageFont
import pytz
import matplotlib.pyplot as plt
from matplotlib.colors import Normalize, ListedColormap
from sklearn.linear_model import LinearRegression

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    filename='combined_script.log',
    level=logging.WARNING,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Constants
API_URL = "https://dmigw.govcloud.dk/v1/radardata/collections/composite/items"
API_KEY = os.getenv('API_KEY')
LIMIT = 40
BBOX = "7.0,54.0,16.0,58.0"
H5_OUTPUT_FOLDER = "h5_files"
PNG_OUTPUT_FOLDER = "png_files"
FORECAST_FOLDER = "forecast_files"
FORECAST_H5_FOLDER = "forecast_h5_files"
AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_KEY')
BUCKET_NAME = os.getenv('AWS_BUCKET_NAME')
REGION_NAME = os.getenv('AWS_REGION')
ENDPOINT_URL = os.getenv('AWS_ENDPOINT_URL')
SUBFOLDER = os.getenv('SUBFOLDER')
REFLECTIVITY_THRESHOLD = 70

# AWS S3 Client
s3_client = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=REGION_NAME,
    endpoint_url=ENDPOINT_URL
)

# Define the colors based on dBZ values
colors = [
    (0.0, 1.0, 1.0, 0.8),  # Cyan for light rain (70 dBZ)
    (0.0, 0.0, 1.0, 0.8),  # Blue for moderate rain (85 dBZ)
    (0.0, 0.0, 0.5, 0.8),  # Dark blue for moderate to heavy rain (100 dBZ)
    (1.0, 1.0, 0.0, 0.8),  # Yellow for heavy rain (128 dBZ)
    (1.0, 0.65, 0.0, 0.8), # Orange for very heavy rain (160 dBZ)
    (1.0, 0.0, 0.0, 0.8),  # Red for extreme rain/hail (192 dBZ)
    (0.5, 0.0, 0.5, 0.8)   # Purple for intense hail (255 dBZ)
]

dbz_values = [70, 85, 100, 128, 160, 192, 255]

cmap = ListedColormap(colors)
norm = Normalize(vmin=70, vmax=255)

months_translation = {
    "January": "januar",
    "February": "februar",
    "March": "marts",
    "April": "april",
    "May": "maj",
    "June": "juni",
    "July": "juli",
    "August": "august",
    "September": "september",
    "October": "oktober",
    "November": "november",
    "December": "december"
}

# Utility Functions
def check_internet_connection():
    try:
        requests.get("http://www.google.com", timeout=5)
        return True
    except requests.ConnectionError:
        logging.warning("No internet connection available.")
        return False

def sanitize_filename(filename):
    return re.sub(r'[\\/:*?"<>|]', '', filename)

def get_current_utc_time():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def convert_utc_to_danish(utc_time):
    utc = pytz.utc
    danish_tz = pytz.timezone('Europe/Copenhagen')
    return utc.localize(utc_time).astimezone(danish_tz)

def translate_month_to_danish(date_str):
    for eng_month, dan_month in months_translation.items():
        date_str = date_str.replace(eng_month, dan_month)
    return date_str

def add_timestamp(image, timestamp, is_forecast=False):
    draw = ImageDraw.Draw(image)
    font_size = 45
    try:
        font = ImageFont.truetype("static/TV2.ttf", font_size)
    except IOError:
        font = ImageFont.load_default()
    timestamp_text = timestamp.strftime('%d. %B %Y - %H:%M')
    timestamp_text = translate_month_to_danish(timestamp_text)
    text = "Prognose" if is_forecast else ""
    full_text = f"{timestamp_text}\n{text}" if text else timestamp_text
    text_bbox = draw.textbbox((0, 0), full_text, font=font)
    text_width = text_bbox[2] - text_bbox[0]
    text_height = text_bbox[3] - text_bbox[1]
    margin = 10
    x = margin
    y = image.size[1] - text_height - margin
    draw.text((x, y), full_text, font=font, fill="black")
    return image

def extract_timestamp(filename):
    try:
        timestamp_str = filename.split('.')[0]
        return datetime.strptime(timestamp_str, '%Y-%m-%dT%H-%M-%SZ')
    except ValueError as e:
        logging.error(f"Error parsing timestamp from filename {filename}: {e}")
        return None

# Download HDF5 Files
def fetch_latest_radar_data():
    try:
        current_utc_time = get_current_utc_time()
        params = {
            'api-key': API_KEY,
            'limit': LIMIT,
            'datetime': f"../{current_utc_time}",
            'bbox': BBOX
        }
        response = requests.get(API_URL, params=params)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        logging.error(f"Error fetching radar data: {e}")
        return None

def should_skip_file(file_datetime):
    return int(file_datetime[14:16]) % 10 == 5

def download_hdf5_files():
    print("Starting download of HDF5 files...")
    data = fetch_latest_radar_data()
    if data is None:
        return

    if not os.path.exists(H5_OUTPUT_FOLDER):
        os.makedirs(H5_OUTPUT_FOLDER)

    fetched_files = set()
    for feature in data['features']:
        file_datetime = feature['properties']['datetime']
        if should_skip_file(file_datetime):
            continue

        file_datetime = file_datetime.replace(":", "-")
        file_name = f"{file_datetime}.h5"
        sanitized_file_name = sanitize_filename(file_name)
        output_path = os.path.join(H5_OUTPUT_FOLDER, sanitized_file_name)
        fetched_files.add(sanitized_file_name)

        if os.path.exists(output_path):
            continue

        hdf5_url = feature['asset']['data']['href']
        try:
            response = requests.get(hdf5_url, stream=True)
            response.raise_for_status()
            with open(output_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
        except Exception as e:
            logging.error(f"Error downloading file: {e}")

    existing_files = set(os.listdir(H5_OUTPUT_FOLDER))
    for file in existing_files - fetched_files:
        try:
            os.remove(os.path.join(H5_OUTPUT_FOLDER, file))
        except Exception as e:
            logging.error(f"Error deleting file: {e}")
    print("Completed download of HDF5 files.")

# Convert HDF5 files to PNG
def convert_hdf5_to_png():
    print("Starting conversion of HDF5 to PNG...")
    if not os.path.exists(PNG_OUTPUT_FOLDER):
        os.makedirs(PNG_OUTPUT_FOLDER)

    hdf5_files = [os.path.join(H5_OUTPUT_FOLDER, file) for file in os.listdir(H5_OUTPUT_FOLDER) if file.endswith('.h5')]
    existing_png_files = {os.path.splitext(f)[0] for f in os.listdir(PNG_OUTPUT_FOLDER) if f.endswith('.png')}
    new_png_files = set()

    for file_path in hdf5_files:
        file_name_without_ext = os.path.splitext(os.path.basename(file_path))[0]
        new_png_files.add(file_name_without_ext)

        if file_name_without_ext in existing_png_files:
            continue

        try:
            with h5py.File(file_path, 'r') as file:
                data = file['dataset1']['data1']['data'][:]
                data[data < REFLECTIVITY_THRESHOLD] = 0  # Remove values below threshold
                data[data == 255] = 0  # Remove 255 values

                # Normalize and map data to colormap
                norm_data = np.clip((data - REFLECTIVITY_THRESHOLD) / (255 - REFLECTIVITY_THRESHOLD), 0, 1)
                mapped_data = cmap(norm_data)
                rgba_data = (mapped_data * 255).astype(np.uint8)
                rgba_data[..., 3] = np.where(data == 0, 0, 204)  # Set transparency for 0 values, 204 for 0.8 alpha

                radar_image = Image.fromarray(rgba_data, 'RGBA')

                # Resize the image to 1280 pixels in width
                width, height = radar_image.size
                new_height = int(height * (1280 / width))
                radar_image = radar_image.resize((1280, new_height), Image.Resampling.LANCZOS)
                
                # Debug: Print image size after resizing
                print(f"Resized image size: {radar_image.size}")

                try:
                    utc_time = datetime.strptime(file_name_without_ext, '%Y-%m-%dT%H-%M-%SZ')
                    danish_time = convert_utc_to_danish(utc_time)
                    radar_image = add_timestamp(radar_image, danish_time)
                except ValueError as e:
                    logging.error(f"Error parsing timestamp: {e}")

                png_path = os.path.join(PNG_OUTPUT_FOLDER, file_name_without_ext + ".png")
                
                # Debug: Print file path before saving
                print(f"Saving PNG file to: {png_path}")
                
                radar_image.save(png_path)
                
                # Debug: Confirm file is saved
                if os.path.exists(png_path):
                    print(f"File saved successfully: {png_path}")
                else:
                    print(f"Failed to save file: {png_path}")
        except FileNotFoundError as e:
            logging.error(f"File not found error: {e}")
        except Exception as e:
            logging.error(f"An unexpected error occurred: {e}")

    for png_file in existing_png_files:
        if png_file not in new_png_files:
            try:
                os.remove(os.path.join(PNG_OUTPUT_FOLDER, png_file + ".png"))
            except Exception as e:
                logging.error(f"Error deleting file: {e}")
    print("Completed conversion of HDF5 to PNG.")

# Generate Forecast Images using Linear Regression
def read_hdf5_data(file_path):
    with h5py.File(file_path, 'r') as file:
        data = file['dataset1']['data1']['data'][:]
        data[data == 255] = 0
        data[data < REFLECTIVITY_THRESHOLD] = 0
    return data

def generate_linear_forecast(datasets):
    X = np.arange(len(datasets)).reshape(-1, 1)
    y = np.array(datasets)
    
    model = LinearRegression()
    model.fit(X, y)
    
    X_forecast = np.arange(len(datasets), len(datasets) + 6).reshape(-1, 1)
    forecast = model.predict(X_forecast)
    
    # Clip forecast values to ensure they are within a realistic range
    forecast = np.clip(forecast, np.min(y), np.max(y))
    
    return forecast

def save_forecast_to_hdf5(forecast, base_file_name, data_shape):
    if not os.path.exists(FORECAST_H5_FOLDER):
        os.makedirs(FORECAST_H5_FOLDER)

    for i, forecast_value in enumerate(forecast):
        forecast_data = np.full(data_shape, forecast_value)
        forecast_data[forecast_data < REFLECTIVITY_THRESHOLD] = 0
        
        file_name = f"{base_file_name}_forecast_{i+1}.h5"
        file_path = os.path.join(FORECAST_H5_FOLDER, file_name)
        
        with h5py.File(file_path, 'w') as h5file:
            dataset = h5file.create_dataset('dataset1/data1/data', data=forecast_data)
            
        print(f"Saved forecast data to {file_path}")

def generate_forecast():
    print("Starting generation of forecast images...")
    if not os.path.exists(FORECAST_FOLDER):
        os.makedirs(FORECAST_FOLDER)

    hdf5_files = [os.path.join(H5_OUTPUT_FOLDER, file) for file in os.listdir(H5_OUTPUT_FOLDER) if file.endswith('.h5')]
    hdf5_files.sort()
    latest_files = hdf5_files[-10:]

    if len(latest_files) < 10:
        logging.error("Not enough data files to generate forecast.")
        return

    datasets = []
    for file_path in latest_files:
        data = read_hdf5_data(file_path)
        datasets.append(np.mean(data))  # Simplified: using mean reflectivity for demonstration

    forecast = generate_linear_forecast(datasets)

    base_file_name = os.path.basename(latest_files[-1]).split('.')[0]
    save_forecast_to_hdf5(forecast, base_file_name, data.shape)

    convert_forecast_hdf5_to_png()

def convert_forecast_hdf5_to_png():
    print("Starting conversion of forecast HDF5 to PNG...")
    if not os.path.exists(FORECAST_FOLDER):
        os.makedirs(FORECAST_FOLDER)

    forecast_hdf5_files = [os.path.join(FORECAST_H5_FOLDER, file) for file in os.listdir(FORECAST_H5_FOLDER) if file.endswith('.h5')]
    forecast_hdf5_files.sort()

    existing_forecast_files = {os.path.splitext(f)[0] for f in os.listdir(FORECAST_FOLDER) if f.endswith('.png')}
    new_forecast_files = set()

    for file_path in forecast_hdf5_files:
        file_name_without_ext = os.path.splitext(os.path.basename(file_path))[0]
        new_forecast_files.add(file_name_without_ext)

        try:
            with h5py.File(file_path, 'r') as file:
                data = file['dataset1']['data1']['data'][:]
                data[data == 255] = 0  # Remove 255 values

                # Debugging output for checking data values
                print(f"Forecast data from {file_path}:")
                print(f"Min value: {np.min(data)}, Max value: {np.max(data)}")

                # Normalize and map data to colormap
                norm_data = np.clip((data - REFLECTIVITY_THRESHOLD) / (255 - REFLECTIVITY_THRESHOLD), 0, 1)
                mapped_data = cmap(norm_data)
                rgba_data = (mapped_data * 255).astype(np.uint8)
                rgba_data[..., 3] = np.where(data == 0, 0, 204)  # Set transparency for 0 values, 204 for 0.8 alpha

                forecast_image = Image.fromarray(rgba_data, 'RGBA')

                # Resize the image to 1280 pixels in width
                width, height = forecast_image.size
                new_height = int(height * (1280 / width))
                forecast_image = forecast_image.resize((1280, new_height), Image.Resampling.LANCZOS)

                try:
                    utc_time = extract_timestamp(file_name_without_ext)
                    if utc_time:
                        forecast_time = convert_utc_to_danish(utc_time)
                        forecast_image = add_timestamp(forecast_image, forecast_time, is_forecast=True)
                except ValueError as e:
                    logging.error(f"Error parsing timestamp: {e}")

                png_path = os.path.join(FORECAST_FOLDER, file_name_without_ext + ".png")
                
                # Debug: Print file path before saving
                print(f"Saving PNG file to: {png_path}")
                
                forecast_image.save(png_path)
                
                # Debug: Confirm file is saved
                if os.path.exists(png_path):
                    print(f"File saved successfully: {png_path}")
                else:
                    print(f"Failed to save file: {png_path}")
        except FileNotFoundError as e:
            logging.error(f"File not found error: {e}")
        except Exception as e:
            logging.error(f"An unexpected error occurred: {e}")

    for forecast_file in existing_forecast_files:
        if forecast_file not in new_forecast_files:
            try:
                os.remove(os.path.join(FORECAST_FOLDER, forecast_file + ".png"))
            except Exception as e:
                logging.error(f"Error deleting file: {e}")
    print("Completed conversion of forecast HDF5 to PNG.")


# Upload Files to S3
def upload_and_rename_files(folder, start_index):
    print(f"Starting upload of files from {folder}...")
    files = [f for f in os.listdir(folder) if f.endswith('.png')]
    files_with_timestamps = [(f, extract_timestamp(f)) for f in files]
    files_with_timestamps = [f for f in files_with_timestamps if f[1] is not None]

    files_with_timestamps.sort(key=lambda x: x[1])

    for idx, (filename, _) in enumerate(files_with_timestamps):
        file_path = os.path.join(folder, filename)
        new_file_name = f"{start_index + idx}.png"
        s3_key = os.path.join(SUBFOLDER, new_file_name)
        try:
            s3_client.upload_file(file_path, BUCKET_NAME, s3_key, ExtraArgs={'ACL': 'public-read'})
        except Exception as e:
            logging.error(f"Error uploading file {file_path} to S3: {e}")
    print(f"Completed upload of files from {folder}.")

# Main Loop and Execution
def main():
    while True:
        if check_internet_connection():
            download_hdf5_files()
            convert_hdf5_to_png()
            generate_forecast()
            upload_and_rename_files(PNG_OUTPUT_FOLDER, 1)
            upload_and_rename_files(FORECAST_FOLDER, 21)
        print("Pausing for 5 minutes...")
        time.sleep(300)

if __name__ == "__main__":
    main()
