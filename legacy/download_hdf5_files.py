import os
import re
import requests
import urllib.parse
import logging
from datetime import datetime, timezone
from dotenv import load_dotenv
load_dotenv()

# Configure logging
logging.basicConfig(
    filename='download.log',  # Specify the file where logs will be written
    level=logging.ERROR,  # Set the logging level to ERROR to log only error messages
    format='%(asctime)s - %(levelname)s - %(message)s'  # Define the format of log messages
)

# Define constants
api_url = "https://dmigw.govcloud.dk/v1/radardata/collections/composite/items"
api_key = os.getenv('API_KEY')
limit = 40
bbox = "7.0,54.0,16.0,58.0"  # Bounding box parameters
h5_output_folder = "h5_files"
png_output_folder = "png_files"

# Check for internet connection
def check_internet_connection():
    try:
        requests.get("http://www.google.com", timeout=5)
        return True
    except requests.ConnectionError:
        return False

# Sanitize filename
def sanitize_filename(filename):
    sanitized_filename = re.sub(r'[\\/:*?"<>|]', '', filename)
    return sanitized_filename

# Get current UTC time formatted for the API call
def get_current_utc_time():
    now = datetime.now(timezone.utc)
    return now.strftime("%Y-%m-%dT%H:%M:%SZ")

# Fetch the latest radar data from the API
def fetch_latest_radar_data(api_url, api_key, limit, bbox):
    try:
        print("Fetching the latest radar data from the API...")
        current_utc_time = get_current_utc_time()
        params = {
            'api-key': api_key,
            'limit': limit,
            'datetime': f"../{current_utc_time}",
            'bbox': bbox
        }
        # Construct the full URL with parameters for debugging
        full_url = requests.Request('GET', api_url, params=params).prepare().url
        print(f"Constructed URL: {full_url}")
        
        response = requests.get(api_url, params=params)
        response.raise_for_status()

        data = response.json()
        print("API response received successfully.")
        return data
    except requests.RequestException as e:
        print(f"Error fetching radar data from the API: {e}")
        return None
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return None

# Skip files based on minute marker
def should_skip_file(file_datetime):
    minute_marker = int(file_datetime[14:16])  # Extract the minute part
    return minute_marker % 10 == 5

# Download HDF5 files
def download_hdf5_files(data, output_folder):
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    fetched_files = set()
    for feature in data['features']:
        file_datetime = feature['properties']['datetime']
        if should_skip_file(file_datetime):
            print(f"Skipping file with datetime {file_datetime}")
            continue

        file_datetime = file_datetime.replace(":", "-")
        file_name = f"{file_datetime}.h5"
        sanitized_file_name = sanitize_filename(file_name)
        output_path = os.path.join(output_folder, sanitized_file_name)
        fetched_files.add(sanitized_file_name)

        # Check if the file already exists
        if os.path.exists(output_path):
            print(f"File {sanitized_file_name} already exists, skipping download.")
            continue

        hdf5_url = feature['asset']['data']['href']
        try:
            print(f"Downloading {sanitized_file_name}...")
            response = requests.get(hdf5_url, stream=True)
            response.raise_for_status()

            with open(output_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            print(f"Downloaded file to: {output_path}")
        except Exception as e:
            print(f"Error downloading file: {e}")

    # Delete files that were not part of the fetch
    existing_files = set(os.listdir(output_folder))
    files_to_delete = existing_files - fetched_files
    for file in files_to_delete:
        try:
            os.remove(os.path.join(output_folder, file))
            print(f"Deleted file: {file}")
        except Exception as e:
            print(f"Error deleting file: {e}")

def main():
    if not check_internet_connection():
        print("Error: No internet connection available.")
        return

    radar_data = fetch_latest_radar_data(api_url, api_key, limit, bbox)
    if radar_data is not None:
        print("Radar data fetched successfully.")
        download_hdf5_files(radar_data, h5_output_folder)
    else:
        print("Failed to fetch radar data. Please check the API URL and key.")

if __name__ == "__main__":
    main()
