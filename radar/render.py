"""Warp radar fields to EPSG:3857 for a MapLibre image overlay and write palette PNGs."""
from pathlib import Path

import numpy as np
from PIL import Image
from pyproj import Transformer
from rasterio.warp import Resampling, calculate_default_transform, reproject

from radar import config


class Warper:
    def __init__(self, crs, transform, shape):
        self.src_crs, self.src_tf = crs, transform
        h, w = shape
        left, top = transform @ (0, 0)
        right, bottom = transform @ (w, h)
        self.dst_tf, self.w, self.h = calculate_default_transform(
            crs, "EPSG:3857", w, h, left=left, bottom=bottom, right=right, top=top)
        x0, y0 = self.dst_tf @ (0, 0)
        x1, y1 = self.dst_tf @ (self.w, self.h)
        to_ll = Transformer.from_crs("EPSG:3857", "EPSG:4326", always_xy=True).transform
        (lon0, lat0), (lon1, lat1) = to_ll(x0, y0), to_ll(x1, y1)
        # MapLibre image source order: TL, TR, BR, BL
        self.coordinates = [[lon0, lat0], [lon1, lat0], [lon1, lat1], [lon0, lat1]]

    def __call__(self, arr: np.ndarray) -> np.ndarray:
        out = np.full((self.h, self.w), np.nan, dtype="float32")
        reproject(arr.astype("float32"), out, src_transform=self.src_tf, src_crs=self.src_crs,
                  dst_transform=self.dst_tf, dst_crs="EPSG:3857", src_nodata=np.nan,
                  dst_nodata=np.nan, resampling=Resampling.nearest)
        return out


def _palette() -> list[int]:
    """Index 0 transparent, 1..len(COLORS) precipitation colours, then the no-coverage grey."""
    rgb = [(0, 0, 0)] + [tuple(int(c[i:i + 2], 16) for i in (1, 3, 5)) for c in config.COLORS]
    rgb.append(config.NODATA_RGBA[:3])
    return [v for px in rgb for v in px]


def save_png(dbz_3857: np.ndarray, path: Path) -> None:
    """Palette PNG: 0 = dry, 1..10 = COLORS by LEVELS bin, 11 = NaN (outside coverage)."""
    n = len(config.COLORS)
    idx = np.digitize(np.nan_to_num(dbz_3857, nan=-99), config.LEVELS[:-1])  # 0 below LEVELS[0]
    idx = np.where(np.isnan(dbz_3857), n + 1, idx).astype(np.uint8)
    im = Image.fromarray(idx, "P")
    im.putpalette(_palette())
    alpha = bytes([0] + [config.PRECIP_ALPHA] * n + [config.NODATA_RGBA[3]])
    im.save(path, optimize=True, transparency=alpha)
