import json
from pathlib import Path

import numpy as np
from pysteps.verification.detcatscores import det_cat_fct
from pysteps.verification.spatialscores import fss

from . import nowcast

THR = 1.0  # mm/t
FSS_SCALE = 20  # px à 500 m = 10 km


def score(fc_dbz: np.ndarray, obs_dbz: np.ndarray) -> dict:
    """CSI og FSS på mm/t, kun hvor observationen er endelig."""
    p, o = nowcast.zr(fc_dbz.astype(np.float64)), nowcast.zr(obs_dbz.astype(np.float64))
    bad = ~np.isfinite(obs_dbz)
    p[bad] = np.nan
    o[bad] = np.nan
    return {
        "csi": float(det_cat_fct(p, o, THR, ["CSI"])["CSI"]),
        "fss": float(fss(p, o, THR, FSS_SCALE)),
    }


def append(path: Path, record: dict) -> None:
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(record) + "\n")
