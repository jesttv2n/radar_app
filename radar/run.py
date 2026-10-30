"""One idempotent worker cycle: list, fetch, nowcast, publish, verify. Run by a systemd timer."""
import contextlib
import io
import json
import logging
import math
import os
import sys
from datetime import datetime, timedelta, timezone

import numpy as np

from . import config, dmi, grid, publish, render

log = logging.getLogger("radar")

LOOKBACK = timedelta(minutes=130)
VERIFY_LEAD = config.LEADS * config.STEP_MIN
VERIFY_KEEP = timedelta(hours=3)
NO_ECHO = -32.0


def _json_safe(record: dict) -> dict:
    return {k: (None if isinstance(v, float) and not math.isfinite(v) else v)
            for k, v in record.items()}


def _read_frames(times):
    """Fetch and crop the listed composites; unusable frames are skipped."""
    frames = []
    for t in times:
        try:
            path = dmi.fetch(t)
        except dmi.DMIError as e:
            log.warning("skip %s: %s", t.isoformat(), e)
            continue
        try:
            c = grid.read(path)
        except Exception as e:  # truncated/corrupt HDF5 must not poison the cache
            log.warning("skip %s: unreadable (%s); deleting %s", t.isoformat(), e, path)
            path.unlink(missing_ok=True)
            continue
        if c.time != t:
            log.warning("skip %s: file says %s", t.isoformat(), c.time.isoformat())
            continue
        frames.append(grid.crop(c, config.BBOX))
    return frames


def _max_dbz(arr) -> float:
    return float(np.nanmax(arr)) if np.isfinite(arr).any() else NO_ECHO


def _verify(vdir, latest, t0_listed):
    """Score the stored lead-60 field made 60 min ago against the newest obs."""
    from . import verify  # lazy: importing pysteps logs a config line
    for p in vdir.glob("fc_*_060.npy"):
        made = datetime.strptime(p.name[3:15], "%Y%m%d%H%M").replace(tzinfo=timezone.utc)
        if made == latest.time - timedelta(minutes=VERIFY_LEAD):
            fc = np.load(p)
            if fc.shape == latest.dbz.shape:
                rec = {"t0": made.isoformat(), "obs_time": latest.time.isoformat(),
                       **verify.score(fc, latest.dbz)}
                verify.append(config.out_dir() / "verification.jsonl", _json_safe(rec))
            p.unlink()
        elif made < t0_listed - VERIFY_KEEP:
            p.unlink()


def _verify_safe(vdir, latest, t0_listed):
    try:
        _verify(vdir, latest, t0_listed)
    except Exception:  # scoring is best effort; never fail the cycle after publishing
        log.exception("verification failed")


def _forecast(frames, latest, warp, public, out):
    """Return [(time, path, max_dbz)] for the lead times, or None when history is too short."""
    from . import nowcast  # lazy: importing pysteps logs a config line
    tail = nowcast.contiguous_tail(frames, config.STEP_MIN)
    if len(tail) < config.MOTION_FRAMES:
        return None
    with contextlib.redirect_stdout(io.StringIO()):  # pysteps is chatty; journald gets our lines only
        fc = nowcast.sprog(tail, config.LEADS)
    forecast = []
    for k in range(config.LEADS):
        t = latest.time + timedelta(minutes=config.STEP_MIN * (k + 1))
        path = public / f"fc_{latest.time:%Y%m%d%H%M}_{config.STEP_MIN * (k + 1):03d}.png"
        render.save_png(warp(fc[k]), path)
        forecast.append((t, path, _max_dbz(fc[k])))
    np.save(out / "verify" / f"fc_{latest.time:%Y%m%d%H%M}_{VERIFY_LEAD:03d}.npy", fc[-1])
    return forecast


def _write_state(path, t0):
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps({"last_t0": t0.isoformat()}), encoding="utf-8")
    os.replace(tmp, path)


def cycle(now: datetime) -> str:
    try:
        return _cycle(now)
    finally:
        try:
            dmi.prune_cache(now)
        except OSError as e:
            log.warning("cache prune failed: %s", e)


def _cycle(now: datetime) -> str:
    out, public = config.out_dir(), config.public_dir()
    state_path = out / "state.json"
    try:
        times = dmi.list_fullrange(now - LOOKBACK)
    except dmi.DMIError as e:
        log.error("listing failed: %s", e)
        return "failed"
    if not times:
        log.error("no composites listed")
        return "failed"
    t0 = times[-1]
    try:
        last = json.loads(state_path.read_text(encoding="utf-8"))["last_t0"]
    except (OSError, ValueError, KeyError):
        last = None
    if last == t0.isoformat():
        log.info("unchanged at %s", t0.isoformat())
        return "unchanged"

    frames = _read_frames(times[-config.HISTORY_FRAMES:])
    if not frames:
        log.error("no usable frames")
        return "failed"
    latest = frames[-1]
    if last and latest.time.isoformat() <= last:  # newest listed frame failed; nothing newer than what is live
        log.info("no newer frame than %s", last)
        return "unchanged"
    public.mkdir(parents=True, exist_ok=True)
    (out / "verify").mkdir(parents=True, exist_ok=True)

    warp = render.Warper(frames[0].crs, frames[0].transform, frames[0].dbz.shape)
    obs = []
    for f in frames:
        path = public / f"obs_{f.time:%Y%m%d%H%M}.png"
        if not path.exists():  # obs names are immutable; served as immutable by nginx
            render.save_png(warp(f.dbz), path)
        obs.append((f.time, path, _max_dbz(f.dbz)))

    try:
        forecast = _forecast(frames, latest, warp, public, out)
    except Exception:
        log.exception("forecast failed; publishing observations only")
        forecast = None

    publish.publish(public, obs, forecast, warp.coordinates, now)
    _write_state(state_path, latest.time)
    _verify_safe(out / "verify", latest, t0)
    log.info("published t0=%s obs=%d forecast=%s", latest.time.isoformat(), len(obs),
             "yes" if forecast else "no")
    return "published"


def main() -> int:
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(levelname)s %(message)s")
    log.info("cycle %s", cycle(datetime.now(timezone.utc)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
