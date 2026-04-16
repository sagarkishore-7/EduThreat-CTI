import pytest


def pytest_addoption(parser):
    """Shared pytest options for contributor and live-source tests."""
    parser.addoption(
        "--source-name",
        action="store",
        default=None,
        help="Specific source name to test (for live source verification).",
    )
    parser.addoption(
        "--max-pages",
        action="store",
        default="1",
        help="Maximum pages to fetch during live source tests (default: 1).",
    )
    parser.addoption(
        "--run-live-sources",
        action="store_true",
        default=False,
        help="Run tests that hit live external source endpoints.",
    )


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "live_source: test hits live external source endpoints and is skipped by default",
    )


def pytest_collection_modifyitems(config, items):
    """Skip live source tests unless they are explicitly enabled."""
    if config.getoption("run_live_sources"):
        return

    skip_live = pytest.mark.skip(
        reason="needs --run-live-sources to hit live external sources",
    )
    for item in items:
        if "live_source" in item.keywords:
            item.add_marker(skip_live)
