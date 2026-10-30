import os
from pathlib import Path

API_BASE = "https://opendataapi.dmi.dk/v1/radardata"
BBOX = (7.6, 54.4, 15.4, 57.9)  # lon_min, lat_min, lon_max, lat_max

STEP_MIN = 10
HISTORY_FRAMES = 12  # 2 t observationer
LEADS = 6  # 60 min prognose
MOTION_FRAMES = 3

ZR_A, ZR_B = 200.0, 1.6

# dBZ-grænser; under LEVELS[0] er pixlen gennemsigtig
LEVELS = [10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 200]
COLORS = ["#a6e3ff", "#5ab4f0", "#2a7fd4", "#1f4fa8", "#2fae3c", "#f2e030",
          "#f5a623", "#e8452c", "#b81f5b", "#7a1fa2"]

ATTRIBUTION = "Radardata: DMI (CC BY 4.0)"


def out_dir() -> Path:
    return Path(os.environ.get("RADAR_OUT", "out"))


def public_dir() -> Path:
    """Kun manifest og PNG; serveres af Nginx."""
    return out_dir() / "public"


def cache_dir() -> Path:
    return Path(os.environ.get("RADAR_CACHE", "cache"))
