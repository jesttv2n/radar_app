"""DMI open data: list fullRange composites and download them (cached)."""
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

from radar import config

TIMEOUT = 30
CACHE_KEEP = timedelta(hours=3)


class DMIError(Exception):
    pass


def list_fullrange(since: datetime) -> list[datetime]:
    """UTC times of fullRange composites from `since` on, ascending."""
    url = f"{config.API_BASE}/collections/composite/items"
    params = {"datetime": f"{since.astimezone(timezone.utc):%Y-%m-%dT%H:%M:%SZ}/..",
              "scanType": "fullRange", "limit": 100}
    times = []
    try:
        while url:
            r = requests.get(url, params=params, timeout=TIMEOUT)
            r.raise_for_status()
            body = r.json()
            times += [datetime.fromisoformat(f["properties"]["datetime"].replace("Z", "+00:00"))
                      for f in body.get("features", [])]
            url = next((l["href"] for l in body.get("links", []) if l.get("rel") == "next"), None)
            params = None  # the next link already carries the query
    except (requests.RequestException, KeyError, ValueError) as e:
        raise DMIError(f"list failed: {e}") from e
    return sorted(times)


def fetch(t: datetime) -> Path:
    """Download composite `t` to the cache, reusing an existing file."""
    name = f"dk.com.{t:%Y%m%d%H%M}.500_max.h5"
    path = config.cache_dir() / name
    if path.exists():
        return path
    path.parent.mkdir(parents=True, exist_ok=True)
    part = path.with_name(name + ".part")
    try:
        r = requests.get(f"{config.API_BASE}/download/{name}", timeout=TIMEOUT)
        r.raise_for_status()
        part.write_bytes(r.content)
        os.replace(part, path)
    except (requests.RequestException, OSError) as e:
        part.unlink(missing_ok=True)
        raise DMIError(f"download of {name} failed: {e}") from e
    return path


def prune_cache(now: datetime) -> None:
    """Delete cached composites (and stray .part files) older than CACHE_KEEP, judged by file name time."""
    for p in config.cache_dir().glob("dk.com.*.h5*"):
        try:
            t = datetime.strptime(p.name[7:19], "%Y%m%d%H%M").replace(tzinfo=timezone.utc)
        except ValueError:
            continue
        if t < now - CACHE_KEEP:
            p.unlink(missing_ok=True)
