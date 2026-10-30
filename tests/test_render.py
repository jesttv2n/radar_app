from pathlib import Path

import numpy as np
from PIL import Image
from pyproj import Transformer

from radar import config, grid, render

F1300 = Path(__file__).parent / "fixtures" / "dk.com.202608281300.500_max.h5"
STATIONS = {"Sindal": (10.13511, 57.48876), "Samsø": (10.58549, 55.81201),
            "Rømø": (8.55053, 55.17259), "Stevns": (12.44817, 55.32562),
            "Bornholm": (14.88746, 55.11283)}


def test_stations_land_within_500m():
    c = grid.crop(grid.read(F1300), config.BBOX)
    warp = render.Warper(c.crs, c.transform, c.dbz.shape)
    to_src = Transformer.from_crs("EPSG:4326", c.crs, always_xy=True).transform
    to_ll = Transformer.from_crs("EPSG:3857", "EPSG:4326", always_xy=True).transform
    errors_m = []
    for lon, lat in STATIONS.values():
        col, row = ~c.transform @ to_src(lon, lat)
        marker = np.zeros(c.dbz.shape, "float32")
        marker[int(row) - 1:int(row) + 2, int(col) - 1:int(col) + 2] = 1  # 3x3 px
        rr, cc = np.nonzero(np.nan_to_num(warp(marker)))
        x, y = warp.dst_tf @ (cc.mean() + 0.5, rr.mean() + 0.5)
        lon2, lat2 = to_ll(x, y)
        errors_m.append(1000 * np.hypot((lon2 - lon) * 111.3 * np.cos(np.radians(lat)),
                                        (lat2 - lat) * 111.3))
    print("station errors (m):", [round(e) for e in errors_m])
    assert max(errors_m) < 500


def test_coordinates_order_and_extent():
    c = grid.crop(grid.read(F1300), config.BBOX)
    (tl, tr, br, bl) = render.Warper(c.crs, c.transform, c.dbz.shape).coordinates
    assert tl[1] == tr[1] > br[1] == bl[1]
    assert tl[0] == bl[0] < tr[0] == br[0]


def test_png_is_small_palette_with_transparency(tmp_path):
    c = grid.crop(grid.read(F1300), config.BBOX)
    arr = render.Warper(c.crs, c.transform, c.dbz.shape)(c.dbz)
    arr[0, 0], arr[0, 1], arr[0, 2] = -32, np.nan, 57
    render.save_png(arr, tmp_path / "x.png")
    im = Image.open(tmp_path / "x.png")
    assert im.mode == "P" and "transparency" in im.info
    assert im.getpixel((0, 0)) == 0 and im.getpixel((1, 0)) == 11
    assert im.getpixel((2, 0)) == 10
    assert (tmp_path / "x.png").stat().st_size < 400_000
    print("png bytes:", (tmp_path / "x.png").stat().st_size)


def test_coordinates_place_stations_within_500m():
    """Pixel from the 4 manifest corners alone (linear in EPSG:3857, as MapLibre draws) vs warped marker."""
    c = grid.crop(grid.read(F1300), config.BBOX)
    warp = render.Warper(c.crs, c.transform, c.dbz.shape)
    (tl, _, br, _) = warp.coordinates
    assert tl == np.array(tl).tolist() and abs(tl[0] - 7.30962) < 1e-3 and abs(tl[1] - 57.99430) < 1e-3
    assert abs(br[0] - 15.86643) < 1e-3 and abs(br[1] - 54.30402) < 1e-3
    to_m = Transformer.from_crs("EPSG:4326", "EPSG:3857", always_xy=True).transform
    to_src = Transformer.from_crs("EPSG:4326", c.crs, always_xy=True).transform
    (xl, yt), (xr, yb) = to_m(*tl), to_m(*br)
    px_m = (xr - xl) / warp.w
    for name, (lon, lat) in STATIONS.items():
        x, y = to_m(lon, lat)
        px, py = (x - xl) / (xr - xl) * warp.w, (yt - y) / (yt - yb) * warp.h
        col, row = ~c.transform @ to_src(lon, lat)
        marker = np.zeros(c.dbz.shape, "float32")
        marker[int(row) - 1:int(row) + 2, int(col) - 1:int(col) + 2] = 1
        rr, cc = np.nonzero(np.nan_to_num(warp(marker)))
        d_px = np.hypot(cc.mean() + 0.5 - px, rr.mean() + 0.5 - py)
        assert d_px < 1, (name, d_px)
        assert d_px * px_m * np.cos(np.radians(lat)) < 500, name


def test_save_png_is_atomic_and_leaves_no_tmp(tmp_path):
    arr = np.zeros((4, 4), "float32")
    render.save_png(arr, tmp_path / "a.png")
    assert [p.name for p in tmp_path.iterdir()] == ["a.png"]
