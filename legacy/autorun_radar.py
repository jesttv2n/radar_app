import subprocess
import time
import logging

# Konfigurer logging
logging.basicConfig(
    filename="autorun.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

# Definer scripts
scripts = [
    ("download_hdf5_files.py", 300),
    ("convert_hdf5_to_png.py", 300),
    ("forecast.py", 300),
    ("s3_upload.py", 300),
]


def run_script(script_name):
    try:
        logging.debug(f"Running {script_name}...")
        result = subprocess.run(["python", script_name], capture_output=True, text=True)
        if result.returncode == 0:
            logging.debug(f"Successfully ran {script_name}")
            return True
        else:
            logging.error(f"Error running {script_name}: {result.stderr}")
            return False
    except Exception as e:
        logging.error(f"Exception running {script_name}: {e}")
        return False


def main():
    while True:

        for script_name, interval in scripts:
            print(run_script(script_name))
        print('Venter')
        time.sleep(300)

if __name__ == "__main__":
    main()
