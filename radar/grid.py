"""Read georeferenced ODIM HDF5 composites and crop them to the map area."""
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import h5py
import numpy as np
from affine import Affine
from pyproj import Transformer
from rasterio.crs import CRS


@dataclass
class Composite:
    time: datetime
    dbz: np.ndarray  # float32, NaN = outside coverage, -32 = no echo
    crs: CRS
    transform: Affine


def _grid_transform(where):
    """Affine for the full composite: the where-corners are pixel centres."""
    proj = where["projdef"].decode()
    x, y = Transformer.from_crs("EPSG:4326", proj, always_xy=True).transform(
        where["UL_lon"][0], where["UL_lat"][0])
    xs, ys = float(where["xscale"]), float(where["yscale"])
    return CRS.from_proj4(proj), Affine(xs, 0, x - xs / 2, 0, -ys, y + ys / 2)


def read(path: Path) -> Composite:
    with h5py.File(path) as f:
        what, where = f["what"].attrs, f["where"].attrs
        raw = f["dataset1/data1/data"][:]
        date, hhmm = what["date"], what["time"]
        gain, offset = float(what["gain"]), float(what["offset"])
        crs, tf = _grid_transform(where)
    date, hhmm = (s.decode() if isinstance(s, bytes) else s for s in (date, hhmm))
    dbz = (raw * np.float32(gain) + np.float32(offset)).astype(np.float32)
    dbz[raw == 0] = -32.0  # undetect: valid, no echo
    dbz[raw == 255] = np.nan  # outside coverage
    t = datetime.strptime(date + hhmm[:4], "%Y%m%d%H%M").replace(tzinfo=timezone.utc)
    return Composite(t, dbz, crs, tf)


def crop(c: Composite, bbox: tuple[float, float, float, float]) -> Composite:
    """Smallest row/col rectangle covering the bbox corners and edge midpoints."""
    x0, y0, x1, y1 = bbox
    xm, ym = (x0 + x1) / 2, (y0 + y1) / 2
    pts = [(x0, y0), (xm, y0), (x1, y0), (x0, ym), (x1, ym), (x0, y1), (xm, y1), (x1, y1)]
    t = Transformer.from_crs("EPSG:4326", c.crs, always_xy=True)
    inv = ~c.transform
    cols, rows = zip(*(inv @ t.transform(lon, lat) for lon, lat in pts))
    h, w = c.dbz.shape
    c0, c1 = max(0, math.floor(min(cols))), min(w, math.ceil(max(cols)))
    r0, r1 = max(0, math.floor(min(rows))), min(h, math.ceil(max(rows)))
    return Composite(c.time, c.dbz[r0:r1, c0:c1], c.crs, c.transform @ Affine.translation(c0, r0))
