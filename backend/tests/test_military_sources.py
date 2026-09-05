"""Military ADS-B sources: adsb.fi first, operator skip list via SB_MIL_SKIP_SOURCES."""

from services.fetchers import military


def test_default_order_starts_with_adsb_fi():
    names = [n for n, _ in military._military_sources("")]
    assert names[0] == "adsb.fi"
    assert set(names) == {"adsb.fi", "adsb.lol", "airplanes.live"}


def test_skip_list_case_insensitive_and_trimmed():
    names = [n for n, _ in military._military_sources(" ADSB.LOL, airplanes.live ,")]
    assert names == ["adsb.fi"]


def test_skip_reads_env(monkeypatch):
    monkeypatch.setenv("SB_MIL_SKIP_SOURCES", "adsb.fi")
    names = [n for n, _ in military._military_sources()]
    assert "adsb.fi" not in names and len(names) == 2
