import h5py
import numpy as np
import os
import logging
import matplotlib.pyplot as plt
from matplotlib.colors import Normalize, ListedColormap
from PIL import Image, ImageDraw, ImageFont
from datetime import datetime
import pytz

# Configure logging
logging.basicConfig(
    filename='convert.log',  # Specify the file where logs will be written
    level=logging.DEBUG,  # Set the logging level to DEBUG to log more details
    format='%(asctime)s - %(levelname)s - %(message)s'  # Define the format of log messages
)

# Define constants
h5_output_folder = "h5_files"
png_output_folder = "png_files"

# Function to add timestamp to an image
def add_timestamp(image, timestamp):
    draw = ImageDraw.Draw(image)
    font_size = 45
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
    return image

# Function to convert UTC to Danish time
def convert_utc_to_danish(utc_time):
    utc = pytz.utc
    danish_tz = pytz.timezone('Europe/Copenhagen')
    utc_dt = utc.localize(utc_time)
    danish_dt = utc_dt.astimezone(danish_tz)
    return danish_dt

# Create a custom color map for dBZ values
# Adjusted to fit the 0-255 range
colors = [
    (0.9, 0.9, 0.9, 0.0),  # Transparent for values below threshold
    (0.0, 1.0, 1.0),  # Cyan for light rain
    (0.0, 0.0, 1.0),  # Blue for moderate rain
    (0.0, 0.0, 0.5),  # Dark blue for moderate to heavy rain
    (1.0, 1.0, 0.0),  # Yellow for very heavy rain
    (1.0, 0.65, 0.0),  # Orange for intense rain
    (1.0, 0.0, 0.0),  # Red for extreme rain/hail
    (0.5, 0.0, 0.5)   # Purple for intense hail
]
dbz_values = [0, 32, 64, 96, 128, 160, 192, 255]
cmap = ListedColormap(colors)
norm = Normalize(vmin=min(dbz_values), vmax=max(dbz_values))

# Convert HDF5 files to PNGs
def convert_hdf5_to_png(hdf5_files, png_folder):
    logging.debug("Starting convert_hdf5_to_png function")
    if not os.path.exists(png_folder):
        os.makedirs(png_folder)

    existing_png_files = {os.path.splitext(f)[0] for f in os.listdir(png_folder) if f.endswith('.png')}
    new_png_files = set()

    for file_path in hdf5_files:
        file_name_without_ext = os.path.splitext(os.path.basename(file_path))[0]
        new_png_files.add(file_name_without_ext)

        # Skip conversion if PNG already exists
        if file_name_without_ext in existing_png_files:
            print(f"Skipping conversion for {file_name_without_ext}.png as it already exists.")
            logging.debug(f"Skipping conversion for {file_name_without_ext}.png as it already exists.")
            continue

        try:
            logging.debug(f"Processing file: {file_path}")
            with h5py.File(file_path, 'r') as file:
                # Navigate through the groups to get to the dataset
                data = file['dataset1']['data1']['data'][:]
                logging.debug(f"Data shape: {data.shape}")

                # Replace all 255 values with 0
                data[data == 255] = 0

                # Create a normalized color map with custom colors
                mapped_data = cmap(norm(data))

                # Convert to RGBA format (with transparency)
                rgba_data = (mapped_data * 255).astype(np.uint8)
                rgba_data[..., 3] = np.where(data == 0, 0, 255)  # Set transparency for 0 values

                # Convert data to PIL Image
                radar_image = Image.fromarray(rgba_data, 'RGBA')

                # Convert filename timestamp to Danish time and add it to the image
                try:
                    utc_time = datetime.strptime(file_name_without_ext, '%Y-%m-%dT%H-%M-%SZ')
                    danish_time = convert_utc_to_danish(utc_time)
                    radar_image = add_timestamp(radar_image, danish_time)
                except ValueError as e:
                    logging.error(f"Error parsing timestamp: {e}")
                    print(f"Error parsing timestamp: {e}")

                # Save as PNG
                png_path = os.path.join(png_folder, file_name_without_ext + ".png")
                radar_image.save(png_path)
                print(f"Saved PNG file: {png_path}")
                logging.debug(f"Saved PNG file: {png_path}")

        except FileNotFoundError as e:
            logging.error(f"File not found error: {e}")
            print(f"File not found error: {e}")
        except Exception as e:
            logging.error(f"An unexpected error occurred: {e}")
            print(f"An unexpected error occurred: {e}")

    # Delete PNG files that do not match the fetched HDF5 file names
    for png_file in existing_png_files:
        if png_file not in new_png_files:
            try:
                os.remove(os.path.join(png_folder, png_file + ".png"))
                print(f"Deleted PNG file: {png_file}.png")
                logging.debug(f"Deleted PNG file: {png_file}.png")
            except Exception as e:
                logging.error(f"Error deleting file: {e}")
                print(f"Error deleting file: {e}")

def main():
    logging.debug("Starting main function")
    hdf5_files = [os.path.join(h5_output_folder, file) for file in os.listdir(h5_output_folder) if file.endswith('.h5')]
    if hdf5_files:
        logging.debug(f"Found HDF5 files: {hdf5_files}")
        convert_hdf5_to_png(hdf5_files, png_output_folder)
    else:
        print("No HDF5 files found to convert.")
        logging.debug("No HDF5 files found to convert.")

if __name__ == "__main__":
    main()
