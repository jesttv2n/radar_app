from datetime import datetime, timezone

import numpy as np
from pyproj import Transformer

from radar import config, grid

FIX = __import__("pathlib").Path(__file__).parent / "fixtures"
F1300 = FIX / "dk.com.202608281300.500_max.h5"


def to_grid(c, lon, lat):
    return Transformer.from_crs("EPSG:4326", c.crs, always_xy=True).transform(lon, lat)


def pixel(c, lon, lat):
    col, row = ~c.transform @ to_grid(c, lon, lat)
    return col, row


def test_read_values():
    c = grid.read(F1300)
    assert c.dbz.shape == (1728, 1984) and c.dbz.dtype == np.float32
    assert np.isnan(c.dbz).any() and (c.dbz == -32).any()
    assert c.time == datetime(2026, 8, 28, 13, 0, tzinfo=timezone.utc)


def test_corners_are_pixel_centres():
    c = grid.read(F1300)
    col, row = pixel(c, 20.735140174892805, 59.827708427801085)  # UR i where
    assert abs(col - 1983.5) < 0.01 and abs(row - 0.5) < 0.01


def test_crop_covers_bbox_and_keeps_geometry():
    full = grid.read(F1300)
    c = grid.crop(full, config.BBOX)
    assert c.dbz.shape[0] < full.dbz.shape[0] and c.dbz.shape[1] < full.dbz.shape[1]
    assert c.time == full.time and c.crs == full.crs
    # Skagen og Bornholm-radar ligger inden for udsnittet
    for lon, lat in [(10.6, 57.74), (14.887, 55.113)]:
        col, row = pixel(c, lon, lat)
        assert 0 <= col < c.dbz.shape[1] and 0 <= row < c.dbz.shape[0]
    # bbox' hjørner og kantmidtpunkter er dækket
    x0, y0, x1, y1 = config.BBOX
    for lon, lat in [(x0, y0), (x1, y0), (x0, y1), (x1, y1), ((x0 + x1) / 2, y0), ((x0 + x1) / 2, y1)]:
        col, row = pixel(c, lon, lat)
        assert 0 <= col <= c.dbz.shape[1] and 0 <= row <= c.dbz.shape[0]
    # Sindal-pixel i udsnit == Sindal-pixel i fuldt grid minus offset
    sx, sy = to_grid(full, 10.2, 57.46)
    fc, fr = map(int, ~full.transform @ (sx, sy))
    cc, cr = map(int, ~c.transform @ (sx, sy))
    off_c, off_r = fc - cc, fr - cr
    assert off_c >= 0 and off_r >= 0
    sub = full.dbz[off_r:off_r + c.dbz.shape[0], off_c:off_c + c.dbz.shape[1]]
    np.testing.assert_array_equal(sub, c.dbz)
    assert full.dbz[fr, fc] == c.dbz[cr, cc] or (np.isnan(c.dbz[cr, cc]) and np.isnan(full.dbz[fr, fc]))
