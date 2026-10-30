"""S-PROG nowcast (pysteps) på dBZ-composites."""
from datetime import timedelta

import numpy as np
from pysteps import motion, nowcasts
from pysteps.utils import transformation

from . import config
from .grid import Composite

NO_ECHO = -32.0


def zr(dbz: np.ndarray) -> np.ndarray:
    """dBZ -> mm/t (Z = a R^b). NaN bevares."""
    rate = (10 ** (dbz / 10) / config.ZR_A) ** (1 / config.ZR_B)
    rate[dbz < config.MIN_DBZ] = 0.0
    return rate


def to_dbz(rate: np.ndarray) -> np.ndarray:
    """mm/t -> dBZ; under tærsklen -32. NaN bevares."""
    with np.errstate(divide="ignore", invalid="ignore"):
        dbz = 10 * np.log10(config.ZR_A * rate ** config.ZR_B)
    dbz[rate < config.RAIN_THR] = NO_ECHO
    dbz[np.isnan(rate)] = np.nan
    return dbz


def contiguous_tail(frames: list[Composite], step_min: int) -> list[Composite]:
    """Længste hale af frames uden tidshul (frames sorteret stigende)."""
    step = timedelta(minutes=step_min)
    tail = frames[-1:]
    for f in reversed(frames[:-1]):
        if tail[0].time - f.time != step:
            break
        tail.insert(0, f)
    return tail


def sprog(frames: list[Composite], leads: int) -> np.ndarray:
    """Prognose [leads, y, x] i dBZ; -32 = ingen regn, NaN hvor seneste frame er NaN.

    Bruger de sidste MOTION_FRAMES frames; kalderen sender en sammenhængende hale.
    """
    n = config.MOTION_FRAMES
    if len(frames) < n:
        raise ValueError(f"need at least {n} frames, got {len(frames)}")
    last = frames[-n:]
    rate = np.nan_to_num(np.stack([zr(f.dbz) for f in last]), nan=0.0)
    shape = (leads, *rate.shape[1:])
    if not (rate >= config.RAIN_THR).any():
        out = np.full(shape, NO_ECHO, dtype=np.float32)
    else:
        hist_db, meta = transformation.dB_transform(rate, threshold=config.RAIN_THR, zerovalue=-15.0)
        velocity = motion.get_method("LK")(hist_db)
        fc_db = nowcasts.get_method("sprog")(hist_db, velocity, leads, n_cascade_levels=6,
                                             precip_thr=meta["threshold"])
        fc = transformation.dB_transform(fc_db, threshold=-10.0, inverse=True)[0]
        out = to_dbz(np.nan_to_num(fc, nan=0.0)).astype(np.float32)
    out[:, np.isnan(last[-1].dbz)] = np.nan
    return out
