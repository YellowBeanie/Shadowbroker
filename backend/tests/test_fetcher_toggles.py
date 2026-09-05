"""SB_DISABLED_FETCHERS: operator kill switch for individual fetchers (pure module)."""

from services import fetcher_toggles as ft


def fetch_satnogs():
    return "satnogs"


def fetch_tinygs():
    return "tinygs"


def fetch_news():
    return "news"


def test_disabled_fetchers_parses_with_and_without_prefix():
    assert ft.disabled_fetchers("satnogs, fetch_tinygs ,, ") == frozenset({"satnogs", "tinygs"})
    assert ft.disabled_fetchers("") == frozenset()
    assert ft.disabled_fetchers("FETCH_SATNOGS") == frozenset({"satnogs"})


def test_disabled_fetchers_reads_env(monkeypatch):
    monkeypatch.setenv(ft.ENV_VAR, "tinygs")
    assert ft.disabled_fetchers() == frozenset({"tinygs"})
    monkeypatch.delenv(ft.ENV_VAR)
    assert ft.disabled_fetchers() == frozenset()


def test_filter_fetchers_removes_only_listed():
    funcs = [fetch_news, fetch_satnogs, fetch_tinygs]
    kept = ft.filter_fetchers(funcs, frozenset({"satnogs", "tinygs"}), tier="slow-tier")
    assert kept == [fetch_news]


def test_filter_fetchers_noop_when_nothing_disabled():
    funcs = [fetch_news, fetch_satnogs]
    assert ft.filter_fetchers(funcs, frozenset()) == funcs
