def test_constants():
    from radar import config
    assert config.API_BASE == "https://opendataapi.dmi.dk/v1/radardata"
    assert config.BBOX == (7.6, 54.4, 15.4, 57.9)
    assert (config.STEP_MIN, config.HISTORY_FRAMES, config.LEADS) == (10, 12, 6)
    assert len(config.COLORS) == len(config.LEVELS) - 1 == 10


def test_out_dir_env(monkeypatch, tmp_path):
    from radar import config
    monkeypatch.setenv("RADAR_OUT", str(tmp_path))
    assert config.out_dir() == tmp_path
    assert config.public_dir() == tmp_path / "public"
