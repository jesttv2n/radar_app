import json
import os
from datetime import datetime, timedelta, timezone

import pytest

from radar import publish

NOW = datetime(2026, 8, 28, 13, 0, tzinfo=timezone.utc)
COORDS = [[7.0, 58.0], [16.0, 58.0], [16.0, 54.0], [7.0, 54.0]]


def mk(out, name, t, dbz=20.0):
    p = out / name
    p.write_bytes(b"png")
    return (t, p, dbz)


def frames(out, n_obs=3, n_fc=6):
    newest = NOW - timedelta(minutes=5)
    obs = [mk(out, f"obs_{t:%Y%m%d%H%M}.png", t, 30.0 + i)
           for i, t in enumerate(newest - timedelta(minutes=10 * k) for k in reversed(range(n_obs)))]
    fc = [mk(out, f"fc_{newest:%Y%m%d%H%M}_{10 * k:03d}.png", newest + timedelta(minutes=10 * k))
          for k in range(1, n_fc + 1)]
    return newest, obs, fc


def test_manifest_schema_and_order(tmp_path):
    newest, obs, fc = frames(tmp_path)
    m = publish.publish(tmp_path, obs[::-1], fc, COORDS, NOW)
    assert json.loads((tmp_path / "manifest.json").read_text()) == m
    assert m["attribution"] == "Radardata: DMI (CC BY 4.0)"
    assert m["coordinates"] == COORDS
    assert m["generated"] == NOW.isoformat()
    assert m["forecast"] == {"t0": newest.isoformat(), "method": "sprog"}
    fr = m["frames"]
    assert [f["time"] for f in fr] == sorted(f["time"] for f in fr)
    assert [f["lead"] for f in fr] == [-20, -10, 0, 10, 20, 30, 40, 50, 60]
    assert [f["kind"] for f in fr] == ["obs"] * 3 + ["forecast"] * 6
    assert fr[2]["url"] == f"obs_{newest:%Y%m%d%H%M}.png"
    assert fr[2]["max_dbz"] == 32.0


def test_nonfinite_max_dbz_is_minus_32(tmp_path):
    _, obs, _ = frames(tmp_path, 1, 0)
    obs[0] = (obs[0][0], obs[0][1], float("nan"))
    m = publish.publish(tmp_path, obs, None, COORDS, NOW)
    assert m["frames"][0]["max_dbz"] == -32.0


def test_publish_is_atomic(tmp_path, monkeypatch):
    _, obs, fc = frames(tmp_path)
    publish.publish(tmp_path, obs, fc, COORDS, NOW)
    seen = []
    real = os.replace

    def spy(src, dst):
        seen.append((os.path.exists(tmp_path / "manifest.json"), str(src)))
        real(src, dst)

    monkeypatch.setattr(os, "replace", spy)
    publish.publish(tmp_path, obs, fc, COORDS, NOW)
    assert seen == [(True, str(tmp_path / "manifest.json.tmp"))]
    assert not (tmp_path / "manifest.json.tmp").exists()
    json.loads((tmp_path / "manifest.json").read_text())


def test_prunes_unreferenced_old_png(tmp_path):
    _, obs, fc = frames(tmp_path)
    old = tmp_path / "obs_old.png"
    recent = tmp_path / "obs_recent.png"
    for p in (old, recent):
        p.write_bytes(b"x")
    os.utime(old, (NOW.timestamp() - 16 * 60,) * 2)
    os.utime(recent, (NOW.timestamp() - 5 * 60,) * 2)
    for _, p, _ in obs + fc:  # referenced files are kept even when old
        os.utime(p, (NOW.timestamp() - 3600,) * 2)
    publish.publish(tmp_path, obs, fc, COORDS, NOW)
    assert not old.exists()
    assert recent.exists()
    assert all(p.exists() for _, p, _ in obs + fc)


def test_forecast_none_gives_null(tmp_path):
    _, obs, _ = frames(tmp_path)
    m = publish.publish(tmp_path, obs, None, COORDS, NOW)
    assert m["forecast"] is None
    assert all(f["kind"] == "obs" and f["lead"] <= 0 for f in m["frames"])
