import json
from datetime import datetime, timedelta, timezone

import numpy as np
import pytest

from radar import config, dmi, run


def T(h, m):
    return datetime(2026, 8, 28, h, m, tzinfo=timezone.utc)


@pytest.fixture
def env(tmp_path, monkeypatch, fixtures_dir):
    monkeypatch.setenv("RADAR_OUT", str(tmp_path / "out"))
    monkeypatch.setenv("RADAR_CACHE", str(tmp_path / "cache"))
    state = {"times": [], "fetched": []}

    def fetch(t):
        p = fixtures_dir / f"dk.com.{t:%Y%m%d%H%M}.500_max.h5"
        if not p.exists():
            raise dmi.DMIError(f"no fixture for {t}")
        state["fetched"].append(t)
        return p

    def lst(since):
        if isinstance(state["times"], Exception):
            raise state["times"]
        return [t for t in state["times"] if t >= since]

    monkeypatch.setattr(dmi, "fetch", fetch)
    monkeypatch.setattr(dmi, "list_fullrange", lst)
    return state


def manifest():
    return json.loads((config.public_dir() / "manifest.json").read_text())


def test_cycle_publishes_then_is_idempotent(env):
    env["times"] = [T(12, 30), T(12, 40), T(12, 50), T(13, 0)]
    assert run.cycle(T(13, 2)) == "published"
    m = manifest()
    kinds = [f["kind"] for f in m["frames"]]
    assert kinds == ["obs"] * 4 + ["forecast"] * 6
    assert m["forecast"]["t0"] == T(13, 0).isoformat()
    assert json.loads((config.out_dir() / "state.json").read_text()) == {"last_t0": T(13, 0).isoformat()}
    assert {p.suffix for p in config.public_dir().iterdir()} == {".png", ".json"}
    before = (config.public_dir() / "manifest.json").read_bytes()
    assert run.cycle(T(13, 4)) == "unchanged"
    assert (config.public_dir() / "manifest.json").read_bytes() == before


def test_cycle_keeps_manifest_when_dmi_fails(env):
    env["times"] = [T(12, 30), T(12, 40), T(12, 50), T(13, 0)]
    assert run.cycle(T(13, 2)) == "published"
    before = (config.public_dir() / "manifest.json").read_bytes()
    env["times"] = dmi.DMIError("down")
    assert run.cycle(T(13, 12)) == "failed"
    assert (config.public_dir() / "manifest.json").read_bytes() == before


def test_gap_in_history_gives_obs_only(env):
    env["times"] = [T(12, 30), T(12, 40), T(13, 0)]  # 12:50 missing
    assert run.cycle(T(13, 2)) == "published"
    m = manifest()
    assert m["forecast"] is None
    assert [f["kind"] for f in m["frames"]] == ["obs"] * 3


def test_corrupt_cache_file_is_deleted_and_skipped(env, tmp_path, fixtures_dir, monkeypatch):
    env["times"] = [T(12, 40), T(12, 50), T(13, 0)]
    bad = tmp_path / "bad.h5"
    bad.write_bytes(b"truncated")
    real = dmi.fetch

    def fetch(t):
        return bad if t == T(12, 50) else real(t)

    monkeypatch.setattr(dmi, "fetch", fetch)
    assert run.cycle(T(13, 2)) == "published"
    assert not bad.exists()
    assert [f["time"] for f in manifest()["frames"]] == [T(12, 40).isoformat(), T(13, 0).isoformat()]


def test_no_usable_frame_is_failed(env):
    env["times"] = [T(13, 30)]  # no fixture -> DMIError on fetch
    assert run.cycle(T(13, 32)) == "failed"
    assert not (config.public_dir() / "manifest.json").exists()


def test_verification_logged_after_60_min(env):
    env["times"] = [T(12, 30), T(12, 40), T(12, 50), T(13, 0)]
    assert run.cycle(T(13, 2)) == "published"
    npy = config.out_dir() / "verify" / "fc_202608281300_060.npy"
    assert npy.exists()
    assert not (config.out_dir() / "verification.jsonl").exists()
    env["times"] = [T(13, 0), T(14, 0)]
    assert run.cycle(T(14, 2)) == "published"
    lines = (config.out_dir() / "verification.jsonl").read_text().splitlines()
    assert len(lines) == 1
    rec = json.loads(lines[0])  # strict: must not contain bare NaN
    assert rec["t0"] == T(13, 0).isoformat() and rec["obs_time"] == T(14, 0).isoformat()
    assert set(rec) >= {"csi", "fss"}
    assert not npy.exists()


def test_old_verify_files_pruned(env):
    env["times"] = [T(12, 30), T(12, 40), T(12, 50), T(13, 0)]
    vdir = config.out_dir() / "verify"
    vdir.mkdir(parents=True)
    old = vdir / "fc_202608280900_060.npy"
    np.save(old, np.zeros((2, 2), dtype=np.float32))
    assert run.cycle(T(13, 2)) == "published"
    assert not old.exists()


def test_json_safe_maps_non_finite_to_none():
    assert run._json_safe({"a": float("nan"), "b": float("inf"), "c": 1.5, "d": "x"}) == \
        {"a": None, "b": None, "c": 1.5, "d": "x"}


def test_nowcast_failure_gives_obs_only(env, monkeypatch):
    def boom(frames, leads):
        raise ValueError("x contains non-finite values")

    monkeypatch.setattr(run.nowcast, "sprog", boom)
    env["times"] = [T(12, 30), T(12, 40), T(12, 50), T(13, 0)]
    assert run.cycle(T(13, 2)) == "published"
    assert manifest()["forecast"] is None
