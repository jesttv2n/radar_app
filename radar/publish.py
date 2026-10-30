import json
import math
import os
from datetime import datetime, timedelta
from pathlib import Path

from . import config

PRUNE_AGE = timedelta(minutes=15)


def _frame(kind, t, path, max_dbz, t0):
    return {
        "time": t.isoformat(),
        "kind": kind,
        "lead": round((t - t0).total_seconds() / 60),
        "url": Path(path).name,
        "max_dbz": float(max_dbz) if math.isfinite(max_dbz) else -32.0,
    }


def publish(out: Path, obs, forecast, coordinates, now: datetime) -> dict:
    """Skriv manifest.json atomisk, og slet derefter gamle ureferencerede PNG'er."""
    out = Path(out)
    t0 = max(t for t, _, _ in obs)
    frames = [_frame("obs", t, p, m, t0) for t, p, m in obs]
    frames += [_frame("forecast", t, p, m, t0) for t, p, m in forecast or []]
    frames.sort(key=lambda f: f["time"])
    manifest = {
        "generated": now.isoformat(),
        "attribution": config.ATTRIBUTION,
        "coordinates": [list(c) for c in coordinates],
        "forecast": {"t0": t0.isoformat(), "method": "sprog"} if forecast else None,
        "frames": frames,
    }
    tmp = out / "manifest.json.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(manifest, f)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, out / "manifest.json")

    keep = {f["url"] for f in frames}
    cutoff = (now - PRUNE_AGE).timestamp()
    for p in out.glob("*.png"):
        if p.name not in keep and p.stat().st_mtime < cutoff:
            p.unlink()
    return manifest
