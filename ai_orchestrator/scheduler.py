import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

def tz_now(tz_name: str) -> datetime:
    return datetime.now(ZoneInfo(tz_name))

def parse_hhmm(hhmm: str):
    h, m = hhmm.strip().split(":")
    return int(h), int(m)

def wave_start_datetime(change_start: datetime, scheduled_hhmm: str) -> datetime:
    h, m = parse_hhmm(scheduled_hhmm)
    candidate = change_start.replace(hour=h, minute=m, second=0, microsecond=0)
    if candidate < change_start:
        candidate = candidate + timedelta(days=1)
    return candidate

def sleep_until(target: datetime, tz_name: str, run_now: bool) -> None:
    if run_now:
        return
    while True:
        now = tz_now(tz_name)
        if now >= target:
            return
        time.sleep(min(30, max(3, int((target-now).total_seconds()))))
