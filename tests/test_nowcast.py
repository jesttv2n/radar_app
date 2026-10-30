import time
from datetime import datetime, timedelta, timezone

import numpy as np
import pytest
from affine import Affine

from radar import config, grid, nowcast

T0 = datetime(2026, 8, 28, 12, 30, tzinfo=timezone.utc)


def make_frame(dbz, t):
    return grid.Composite(t, np.asarray(dbz, dtype=np.float32), None, Affine(500, 0, 0, 0, -500, 0))


def times(n, start=T0):
    return [start + timedelta(minutes=10 * i) for i in range(n)]


def blob_frame(cx, t, size=300, cy=150, sigma=8.0):
    yy, xx = np.mgrid[:size, :size]
    bump = np.exp(-((xx - cx) ** 2 + (yy - cy) ** 2) / (2 * sigma ** 2))
    return make_frame(np.where(bump > 0.01, -32 + 72 * bump, -32.0), t)


def centroid_col(mask):
    return np.nonzero(mask)[1].mean()


def test_dry_history_returns_no_rain(monkeypatch):
    frames = [make_frame(np.full((200, 200), -32.0), t) for t in times(3)]
    monkeypatch.setattr(nowcast.motion, "get_method", lambda *_: pytest.fail("no pysteps when dry"))
    out = nowcast.sprog(frames, 6)
    assert out.shape == (6, 200, 200) and np.all(out[np.isfinite(out)] == -32)


@pytest.mark.filterwarnings("ignore::RuntimeWarning")  # pysteps outlier cleaning on a sparse synthetic field
def test_blob_keeps_moving_east():
    frames = [blob_frame(100 + 4 * i, t) for i, t in enumerate(times(3))]
    last_cx = centroid_col(frames[-1].dbz >= 20)
    out = nowcast.sprog(frames, 3)
    cx = centroid_col(out[2] >= 20)
    assert abs(cx - (last_cx + 12)) <= 3


@pytest.mark.filterwarnings("ignore::RuntimeWarning")  # pysteps outlier cleaning on a sparse synthetic field
def test_nan_follows_latest_frame():
    frames = [blob_frame(100 + 4 * i, t) for i, t in enumerate(times(3))]
    frames[-1].dbz[:, :20] = np.nan
    out = nowcast.sprog(frames, 2)
    assert np.all(np.isnan(out[:, :, :20])) and np.all(np.isfinite(out[:, :, 20:]))


def _dry(t, size=300):
    return make_frame(np.full((size, size), -32.0), t)


@pytest.mark.filterwarnings("ignore::RuntimeWarning")
def test_dry_dry_stray_pixel_does_not_raise():
    stray = _dry(times(3)[2])
    stray.dbz[150, 150] = 40.0
    out = nowcast.sprog([_dry(times(3)[0]), _dry(times(3)[1]), stray], 3)
    assert out.shape == (3, 300, 300) and np.isfinite(out).all()


@pytest.mark.filterwarnings("ignore::RuntimeWarning")
def test_dry_then_rain_still_forecasts_moving_rain():
    t = times(3)
    frames = [_dry(t[0]), blob_frame(104, t[1]), blob_frame(108, t[2])]
    out = nowcast.sprog(frames, 3)
    assert (out[2] >= 20).any()
    assert abs(centroid_col(out[2] >= 20) - (108 + 12)) <= 4


@pytest.mark.filterwarnings("ignore::RuntimeWarning")
def test_rain_then_dry_latest_gives_dry_forecast():
    t = times(3)
    out = nowcast.sprog([blob_frame(100, t[0]), blob_frame(104, t[1]), _dry(t[2])], 3)
    assert np.all(out == -32)


@pytest.mark.filterwarnings("ignore::RuntimeWarning")
def test_identical_rainy_frames_fall_back_to_extrapolation():
    frames = [blob_frame(100, t) for t in times(3)]
    out = nowcast.sprog(frames, 3)
    assert out.shape == (3, 300, 300)
    assert abs(centroid_col(out[2] >= 20) - centroid_col(frames[-1].dbz >= 20)) <= 2


def test_contiguous_tail_drops_before_gap():
    t = times(4)  # 12:30, 12:40, 12:50, 13:00
    frames = [make_frame(np.zeros((2, 2)), x) for x in (t[0], t[1], t[3])]
    assert [f.time for f in nowcast.contiguous_tail(frames, 10)] == [t[3]]


def test_contiguous_tail_keeps_complete_history():
    frames = [make_frame(np.zeros((2, 2)), x) for x in times(4)]
    assert nowcast.contiguous_tail(frames, 10) == frames
    assert nowcast.contiguous_tail([], 10) == []


def test_needs_three_frames():
    frames = [make_frame(np.full((20, 20), -32.0), t) for t in times(2)]
    with pytest.raises(ValueError):
        nowcast.sprog(frames, 6)


def test_zr_roundtrip_and_nan():
    dbz = np.array([np.nan, -32.0, 6.9, 20.0, 40.0], dtype=np.float32)
    rate = nowcast.zr(dbz)
    assert np.isnan(rate[0]) and rate[1] == 0 and rate[2] == 0 and rate[3] > 0
    back = nowcast.to_dbz(rate)
    assert np.isnan(back[0]) and back[1] == -32
    assert back[3] == pytest.approx(20.0, abs=0.01) and back[4] == pytest.approx(40.0, abs=0.01)


@pytest.mark.slow
def test_sprog_runtime(fixtures_dir):
    names = ["202608281230", "202608281240", "202608281250"]
    frames = [grid.crop(grid.read(fixtures_dir / f"dk.com.{n}.500_max.h5"), config.BBOX) for n in names]
    t = time.perf_counter()
    out = nowcast.sprog(frames, config.LEADS)
    dt = time.perf_counter() - t
    print(f"\nsprog runtime on BBOX crop {frames[0].dbz.shape}: {dt:.1f} s")
    assert out.shape == (config.LEADS, *frames[0].dbz.shape)
    assert dt < 90
