"""Keep unrelated daily notifications independent of the hour pytest happens to run."""
import pytest


@pytest.fixture(autouse=True)
def disable_daily_report_unless_a_test_enables_it(monkeypatch):
    # Report-specific tests set cfg.daily_report_hour explicitly. Other scenarios assert
    # source alerts and must not acquire an extra report when the local clock reaches 09:00.
    monkeypatch.setenv('DAILY_REPORT_HOUR', '-1')
