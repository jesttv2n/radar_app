from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
import requests

from radar import config, dmi

UTC = timezone.utc
API = config.API_BASE


def item(ts):
    return {"id": f"dk.com.{ts:%Y%m%d%H%M}.500_max.h5",
            "properties": {"datetime": f"{ts:%Y-%m-%dT%H:%M:%SZ}", "scanType": "fullRange"}}


class Resp:
    def __init__(self, status=200, json_data=None, content=b""):
        self.status_code, self._json, self.content = status, json_data, content

    def json(self):
        return self._json

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"{self.status_code}")

    def iter_content(self, chunk_size=1):
        yield self.content


@pytest.fixture
def fake_get(monkeypatch):
    """Replaces requests.get in radar.dmi; responses are queued in .responses."""
    fake = SimpleNamespace(calls=[], responses=[])

    def get(url, params=None, timeout=None, **kw):
        fake.calls.append(SimpleNamespace(url=url, params=params or {}, timeout=timeout))
        r = fake.responses.pop(0)
        if isinstance(r, Exception):
            raise r
        return r

    monkeypatch.setattr(dmi.requests, "get", get)
    return fake


def test_list_fullrange_builds_query_and_follows_next(fake_get):
    t = lambda m: datetime(2026, 8, 28, 11, m, tzinfo=UTC)
    nxt = f"{API}/collections/composite/items?page=2"
    fake_get.responses = [
        Resp(json_data={"features": [item(t(20)), item(t(0))], "links": [{"rel": "next", "href": nxt}]}),
        Resp(json_data={"features": [item(t(10))], "links": [{"rel": "self", "href": nxt}]}),
    ]
    ts = dmi.list_fullrange(datetime(2026, 8, 28, 11, 0, tzinfo=UTC))
    assert fake_get.calls[0].url == f"{API}/collections/composite/items"
    assert fake_get.calls[0].params["scanType"] == "fullRange"
    assert fake_get.calls[0].params["datetime"] == "2026-08-28T11:00:00Z/.."
    assert fake_get.calls[0].params["limit"] == 100
    assert "api-key" not in fake_get.calls[0].params
    assert fake_get.calls[1].url == nxt
    assert ts == sorted(ts) and len(ts) == 3
    assert all(x.tzinfo is not None and x.utcoffset().total_seconds() == 0 for x in ts)


def test_list_fullrange_error(fake_get):
    fake_get.responses = [Resp(500)]
    with pytest.raises(dmi.DMIError):
        dmi.list_fullrange(datetime(2026, 8, 28, 11, 0, tzinfo=UTC))


T = datetime(2026, 8, 28, 13, 0, tzinfo=UTC)


def test_fetch_uses_cache(tmp_path, monkeypatch, fake_get):
    monkeypatch.setenv("RADAR_CACHE", str(tmp_path))
    fake_get.responses = [Resp(content=b"hdf")]
    p = dmi.fetch(T)
    assert p == tmp_path / "dk.com.202608281300.500_max.h5"
    assert p.read_bytes() == b"hdf"
    assert fake_get.calls[0].url == f"{API}/download/dk.com.202608281300.500_max.h5"
    assert fake_get.calls[0].timeout == 30
    assert dmi.fetch(T) == p
    assert len(fake_get.calls) == 1
    assert not list(tmp_path.glob("*.part"))


def test_fetch_raises_dmierror_on_500(tmp_path, monkeypatch, fake_get):
    monkeypatch.setenv("RADAR_CACHE", str(tmp_path))
    fake_get.responses = [Resp(500)]
    with pytest.raises(dmi.DMIError):
        dmi.fetch(T)
    assert not list(tmp_path.iterdir())


def test_fetch_raises_dmierror_on_timeout(tmp_path, monkeypatch, fake_get):
    monkeypatch.setenv("RADAR_CACHE", str(tmp_path))
    fake_get.responses = [requests.Timeout()]
    with pytest.raises(dmi.DMIError):
        dmi.fetch(T)
