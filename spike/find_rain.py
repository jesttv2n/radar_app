"""Scan DMI composite history and rank timestamps by precipitation over Nordjylland."""
import io
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import h5py
import numpy as np
import requests

BASE = "https://opendataapi.dmi.dk/v1/radardata/download/dk.com.{:%Y%m%d%H%M}.500_max.h5"
# Rough Nordjylland window in the composite grid (row, col); Sindal radar is at ~(607, 792)
ROWS, COLS = slice(520, 800), slice(600, 900)


def score(t):
    try:
        r = requests.get(BASE.format(t), timeout=30)
        r.raise_for_status()
        d = h5py.File(io.BytesIO(r.content))["dataset1/data1/data"][ROWS, COLS]
    except Exception:
        return t, None
    valid = d != 255
    rain = valid & (d >= 2 * (20 + 32))  # >= 20 dBZ
    return t, rain.sum() / max(valid.sum(), 1)


def main(days=175, step_h=3):
    end = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)
    times = [end - timedelta(hours=step_h * i) for i in range(days * 24 // step_h)]
    with ThreadPoolExecutor(16) as ex:
        results = [r for r in ex.map(score, times) if r[1] is not None]
    results.sort(key=lambda r: -r[1])
    print(f"scanned {len(results)}/{len(times)}")
    for t, s in results[:25]:
        print(f"{t:%Y-%m-%d %H:%M}Z  {100 * s:5.1f}% >=20dBZ")


if __name__ == "__main__":
    main(*map(int, sys.argv[1:]))
