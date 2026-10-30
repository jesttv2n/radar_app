"""Round-trip check: mark radar stations in the DMI grid, warp to EPSG:3857, see where they land."""
import glob
import numpy as np
from affine import Affine
from pyproj import Transformer
from run_spike import C0, R0, Warper, grid_transform, load
from datetime import datetime

STATIONS = {"Sindal": (10.13511, 57.48876), "Samsø": (10.58549, 55.81201), "Rømø": (8.55053, 55.17259),
            "Stevns": (12.44817, 55.32562), "Bornholm": (14.88746, 55.11283)}
f = sorted(glob.glob("cache/*.h5"))[0]
t = datetime.strptime(f[6:18], "%Y%m%d%H%M")
dbz, where = load(t)
crs, tf = grid_transform(where)
crop = dbz[R0:1280, C0:1280]
ctf = tf * Affine.translation(C0, R0)
warp = Warper(crs, ctf, crop.shape)
to_src = Transformer.from_crs("EPSG:4326", crs.to_proj4(), always_xy=True).transform
to_ll = Transformer.from_crs("EPSG:3857", "EPSG:4326", always_xy=True).transform
for name, (lon, lat) in STATIONS.items():
    col, row = ~ctf * to_src(lon, lat)
    marker = np.zeros(crop.shape, "float32")
    marker[int(row) - 1:int(row) + 2, int(col) - 1:int(col) + 2] = 1  # 3x3 px = 1.5 km
    out = np.nan_to_num(warp(marker))
    rr, cc = np.nonzero(out)
    x, y = warp.dst_tf * (cc.mean() + 0.5, rr.mean() + 0.5)
    lon2, lat2 = to_ll(x, y)
    err = np.hypot((lon2 - lon) * 111.3 * np.cos(np.radians(lat)), (lat2 - lat) * 111.3)
    print(f"{name:9s} expected {lon:.4f},{lat:.4f}  got {lon2:.4f},{lat2:.4f}  error {err * 1000:.0f} m")
