import json

import numpy as np

from radar import verify


def field():
    f = np.full((60, 60), -32.0, dtype=np.float32)
    f[20:40, 20:40] = 35.0
    return f


def test_score_perfect_is_one():
    s = verify.score(field(), field())
    assert s["csi"] == 1.0 and s["fss"] == 1.0
    assert isinstance(s["csi"], float) and isinstance(s["fss"], float)


def test_score_ignores_nan():
    obs = field()
    obs[:, :10] = np.nan
    fc = field()
    fc[:, :10] = 50.0  # forkert, men uden for obs-dækning
    s = verify.score(fc, obs)
    assert s["csi"] == 1.0 and s["fss"] == 1.0


def test_append_writes_json_lines(tmp_path):
    p = tmp_path / "v.jsonl"
    verify.append(p, {"a": 1})
    verify.append(p, {"b": 2})
    assert [json.loads(x) for x in p.read_text().splitlines()] == [{"a": 1}, {"b": 2}]
