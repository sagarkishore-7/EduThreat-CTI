import asyncio
from unittest.mock import patch

from fastapi.responses import JSONResponse

from src.edu_cti.api.main import health_check


class _StalledWatchdog:
    def is_stalled(self) -> bool:
        return True


def test_health_check_returns_503_when_watchdog_is_stalled():
    with patch(
        "src.edu_cti.pipeline.phase2.__main__._get_watchdog",
        return_value=_StalledWatchdog(),
    ):
        response = asyncio.run(health_check())

    assert isinstance(response, JSONResponse)
    assert response.status_code == 503
    assert b'"status":"degraded"' in response.body
    assert b'"watchdog":"stalled"' in response.body
