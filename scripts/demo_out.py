"""Demo-manifest til lokal test af widgetten: én cyklus mod fixtures (ingen DMI-kald).

Kør i radar-test-containeren: RADAR_OUT=out/demo python scripts/demo_out.py
"""
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
os.environ.setdefault("RADAR_OUT", "out/demo")
os.environ.setdefault("RADAR_CACHE", "out/demo-cache")

from radar import dmi, run  # noqa: E402

FIXTURES = ROOT / "tests" / "fixtures"
TIMES = sorted(datetime.strptime(p.name.split(".")[2], "%Y%m%d%H%M").replace(tzinfo=timezone.utc)
               for p in FIXTURES.glob("dk.com.*.500_max.h5"))
NOW = datetime(2026, 8, 28, 13, 0, tzinfo=timezone.utc)


def fetch(t):
    return FIXTURES / f"dk.com.{t:%Y%m%d%H%M}.500_max.h5"


dmi.fetch = fetch
dmi.list_fullrange = lambda since: [t for t in TIMES if since <= t <= NOW]

if __name__ == "__main__":
    (Path(os.environ["RADAR_OUT"]) / "state.json").unlink(missing_ok=True)  # altid ny publicering
    print(run.cycle(NOW))
