"""Spike: georeferenced DMI radar overlay + pysteps nowcast, verified against what was observed.

Usage (inside the radar-spike container):  python run_spike.py 2026-08-28T13:00
Writes out/<case>/: PNGs in EPSG:3857, manifest.json (bounds, frames, scores).
"""
import json
import os
import sys
from datetime import datetime, timedelta, timezone

import h5py
import numpy as np
import requests
from affine import Affine
from matplotlib.colors import BoundaryNorm, ListedColormap
from PIL import Image
from pyproj import Transformer
from pysteps import motion, nowcasts
from pysteps.utils import transformation
from pysteps.verification.detcatscores import det_cat_fct
from pysteps.verification.spatialscores import fss
from rasterio.crs import CRS
from rasterio.warp import Resampling, calculate_default_transform, reproject

URL = "https://opendataapi.dmi.dk/v1/radardata/download/dk.com.{:%Y%m%d%H%M}.500_max.h5"
# 10 min = fullRange composites only. 5 min mixes in doppler scans, which have a
# different coverage and intensity and make motion and scores zigzag.
STEP = int(os.getenv("STEP", "10"))  # minutes
PAST, LEADS = 60 // STEP, 60 // STEP  # 60 min history, 60 min forecast
# Crop of the 1984x1728 composite covering Denmark (rows, cols)
R0, R1, C0, C1 = 380, 1280, 380, 1280

# dBZ colour scale (light rain -> hail), alpha applied below
LEVELS = [10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 70]
COLORS = ["#a6e3ff", "#5ab4f0", "#2a7fd4", "#1f4fa8", "#2fae3c", "#f2e030",
          "#f5a623", "#e8452c", "#b81f5b", "#7a1fa2"]
CMAP, NORM = ListedColormap(COLORS), BoundaryNorm(LEVELS, len(COLORS))


def load(t):
    os.makedirs("cache", exist_ok=True)
    path = f"cache/{t:%Y%m%d%H%M}.h5"
    if not os.path.exists(path):
        r = requests.get(URL.format(t), timeout=60)
        r.raise_for_status()
        open(path, "wb").write(r.content)
    f = h5py.File(path)
    what, where = f["what"].attrs, f["where"].attrs
    raw = f["dataset1/data1/data"][:]
    dbz = raw * what["gain"] + what["offset"]
    dbz[raw == 0] = -32.0  # undetect: valid, no echo
    dbz[raw == 255] = np.nan  # outside coverage
    return dbz, dict(where)


def grid_transform(where):
    """Affine for the full composite: the where-corners are pixel centres (verified)."""
    proj = where["projdef"].decode()
    x, y = Transformer.from_crs("EPSG:4326", proj, always_xy=True).transform(where["UL_lon"][0], where["UL_lat"][0])
    xs, ys = float(where["xscale"]), float(where["yscale"])
    return CRS.from_proj4(proj), Affine(xs, 0, x - xs / 2, 0, -ys, y + ys / 2)


def zr(dbz, a=200.0, b=1.6):
    """dBZ -> mm/h (Z = a R^b, DMI's own constants)."""
    rate = (10 ** (dbz / 10) / a) ** (1 / b)
    rate[dbz < 7] = 0.0
    return rate


def to_dbz(rate, a=200.0, b=1.6):
    with np.errstate(divide="ignore"):
        dbz = 10 * np.log10(a * rate ** b)
    dbz[rate < 0.1] = -32.0
    return dbz


class Warper:
    def __init__(self, src_crs, src_tf, shape):
        self.src_crs, self.src_tf, self.shape = src_crs, src_tf, shape
        h, w = shape
        left, top = src_tf * (0, 0)
        right, bottom = src_tf * (w, h)
        self.dst_tf, self.w, self.h = calculate_default_transform(
            src_crs, "EPSG:3857", w, h, left=left, bottom=bottom, right=right, top=top)
        x0, y0 = self.dst_tf * (0, 0)
        x1, y1 = self.dst_tf * (self.w, self.h)
        to_ll = Transformer.from_crs("EPSG:3857", "EPSG:4326", always_xy=True).transform
        (lon0, lat0), (lon1, lat1) = to_ll(x0, y0), to_ll(x1, y1)
        # MapLibre image source order: TL, TR, BR, BL
        self.coordinates = [[lon0, lat0], [lon1, lat0], [lon1, lat1], [lon0, lat1]]

    def __call__(self, arr):
        out = np.full((self.h, self.w), np.nan, dtype="float32")
        reproject(arr.astype("float32"), out, src_transform=self.src_tf, src_crs=self.src_crs,
                  dst_transform=self.dst_tf, dst_crs="EPSG:3857", src_nodata=np.nan,
                  dst_nodata=np.nan, resampling=Resampling.nearest)
        return out


def save_png(dbz, path):
    rgba = (CMAP(NORM(np.nan_to_num(dbz, nan=-99))) * 255).astype(np.uint8)
    rgba[..., 3] = np.where(np.nan_to_num(dbz, nan=-99) >= LEVELS[0], 215, 0)
    rgba[np.isnan(dbz)] = (90, 90, 90, 70)  # outside radar coverage: faint grey
    Image.fromarray(rgba, "RGBA").save(path, optimize=True)


def main(t0_str):
    t0 = datetime.fromisoformat(t0_str).replace(tzinfo=timezone.utc)
    case = f"{t0:%Y%m%dT%H%M}_{STEP}min"
    out = f"out/{case}"
    os.makedirs(out, exist_ok=True)

    times = [t0 + timedelta(minutes=STEP * i) for i in range(-PAST, LEADS + 1)]
    frames, where = [], None
    for t in times:
        dbz, where = load(t)
        frames.append(dbz[R0:R1, C0:C1])
    frames = np.array(frames)
    src_crs, full_tf = grid_transform(where)
    crop_tf = full_tf * Affine.translation(C0, R0)
    warp = Warper(src_crs, crop_tf, frames.shape[1:])

    obs_rate = zr(frames)
    past = obs_rate[: PAST + 1]  # up to and including t0
    future_obs = obs_rate[PAST + 1:]
    valid = np.isfinite(frames[PAST + 1:])

    # Nowcasts: fill outside-coverage with 0 for motion/advection, verify only on valid pixels
    hist = np.nan_to_num(past, nan=0.0)
    hist_db, meta = transformation.dB_transform(hist, threshold=0.1, zerovalue=-15.0)
    velocity = motion.get_method("LK")(hist_db[-3:])

    fc = {
        "persistence": np.repeat(hist[-1:], LEADS, axis=0),
        "extrapolation": nowcasts.get_method("extrapolation")(hist[-1], velocity, LEADS),
    }
    sprog_db = nowcasts.get_method("sprog")(hist_db[-3:], velocity, LEADS, n_cascade_levels=6,
                                            precip_thr=meta["threshold"])
    fc["sprog"] = transformation.dB_transform(sprog_db, threshold=-10.0, inverse=True)[0]
    for k in fc:
        fc[k] = np.nan_to_num(fc[k], nan=0.0)

    scores = {k: {"csi_1mm": [], "fss_1mm_10km": []} for k in fc}
    for k, f in fc.items():
        for i in range(LEADS):
            m = valid[i]
            o = np.where(m, future_obs[i], np.nan)
            p = np.where(m, f[i], np.nan)
            scores[k]["csi_1mm"].append(round(float(det_cat_fct(p, o, 1.0, ["CSI"])["CSI"]), 3))
            scores[k]["fss_1mm_10km"].append(round(float(fss(p, o, 1.0, 20)), 3))

    manifest = {"case": case, "t0": t0.isoformat(), "coordinates": warp.coordinates,
                "step_minutes": STEP, "frames": [], "scores": scores,
                "attribution": "Radardata: DMI (CC BY 4.0)"}
    for i, t in enumerate(times):
        lead = round((t - t0).total_seconds() / 60)
        entry = {"time": t.isoformat(), "lead": lead, "obs": f"obs_{lead:+04d}.png"}
        save_png(warp(frames[i]), f"{out}/{entry['obs']}")
        if lead > 0:
            for k, f in fc.items():
                name = f"{k}_{lead:+04d}.png"
                save_png(warp(np.where(np.isfinite(frames[i]), to_dbz(f[lead // STEP - 1]), np.nan)), f"{out}/{name}")
                entry[k] = name
        manifest["frames"].append(entry)
    json.dump(manifest, open(f"{out}/manifest.json", "w"), indent=1)

    print(f"case {case}: bounds {warp.coordinates}")
    print("lead  " + "  ".join(f"{k:>22}" for k in fc))
    for i in range(LEADS):
        print(f"{(i + 1) * STEP:3d}m  " + "  ".join(
            f"CSI {scores[k]['csi_1mm'][i]:.2f} FSS {scores[k]['fss_1mm_10km'][i]:.2f}" for k in fc))


if __name__ == "__main__":
    main(sys.argv[1])
