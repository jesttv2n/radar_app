import os
import boto3
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# AWS Configuration
AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_KEY')
BUCKET_NAME = os.getenv('AWS_BUCKET_NAME')
REGION_NAME = os.getenv('AWS_REGION')
ENDPOINT_URL = os.getenv('AWS_ENDPOINT_URL')  # Custom endpoint URL
SUBFOLDER = os.getenv('SUBFOLDER')

# Define folders
png_folder = "png_files"
forecast_folder = "forecast_files"

# Initialize boto3 client with custom endpoint URL
s3_client = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=REGION_NAME,
    endpoint_url=ENDPOINT_URL  # Add custom endpoint URL here
)

# Function to extract timestamp from filename
def extract_timestamp(filename):
    try:
        # Assuming the filename is in the format 'YYYY-MM-DDTHH-MM-SSZ.png'
        timestamp_str = filename.split('.')[0]
        return datetime.strptime(timestamp_str, '%Y-%m-%dT%H-%M-%SZ')
    except ValueError as e:
        print(f"Error parsing timestamp from filename {filename}: {e}")
        return None

# Function to upload and rename files
def upload_and_rename_files(folder, start_index, subfolder):
    files = [f for f in os.listdir(folder) if f.endswith('.png')]
    # Sort files by timestamp extracted from filename
    files.sort(key=lambda x: extract_timestamp(x))
    
    for idx, filename in enumerate(files):
        file_path = os.path.join(folder, filename)
        new_file_name = f"{start_index + idx}.png"
        s3_key = os.path.join(subfolder, new_file_name)
        s3_client.upload_file(file_path, BUCKET_NAME, s3_key, ExtraArgs={'ACL': 'public-read'})
        print(f"Uploaded {file_path} as {s3_key} to S3.")

def main():
    print("Uploading PNG files...")
    upload_and_rename_files(png_folder, 1, SUBFOLDER)
    print("Uploading forecast PNG files...")
    upload_and_rename_files(forecast_folder, 21, SUBFOLDER)

if __name__ == "__main__":
    main()
