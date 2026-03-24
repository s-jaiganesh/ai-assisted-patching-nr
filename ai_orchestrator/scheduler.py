from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

# Day of the week mapping for parsing
DAY_MAP = {
    "mon": 0, "tue": 1, "wed": 2, "thu": 3, "fri": 4, "sat": 5, "sun": 6
}

def find_date_for_nth_weekday(year: int, month: int, nth_week: int, weekday: int) -> datetime.date:
    """
    Finds the specific date for a schedule like "the 3rd Wednesday of the month".
    `nth_week` is 1-based. `weekday` is 0-based (Monday=0).
    """
    first_day_of_month = datetime(year, month, 1)
    
    # Calculate the date of the first occurrence of the target weekday
    day_of_week_offset = (weekday - first_day_of_month.weekday() + 7) % 7
    first_occurrence_date = first_day_of_month + timedelta(days=day_of_week_offset)
    
    # Calculate the date for the Nth week
    target_date = first_occurrence_date + timedelta(weeks=nth_week - 1)
    
    # If the calculated date has spilled over into the next month, it's not a valid date for the rule.
    if target_date.month != month:
        raise ValueError(f"The {nth_week} week {weekday} for {year}-{month} does not exist.")

    return target_date.date()

def parse_group_name_for_schedule(group_name: str, change_window_start: datetime, tz: ZoneInfo):
    """
    Parses a group name like 'week1_wed_2300' into a specific execution datetime.
    Returns the calculated datetime object or None if the name doesn't match the pattern.
    """
    parts = group_name.lower().strip().split('_')
    if len(parts) != 3 or not parts[0].startswith('week'):
        return None

    try:
        week_num_str = parts[0][4:]
        nth_week = int(week_num_str)
        
        day_str = parts[1]
        if day_str not in DAY_MAP:
            return None
        weekday = DAY_MAP[day_str]
        
        time_str = parts[2]
        if len(time_str) != 4:
            return None
        hour = int(time_str[:2])
        minute = int(time_str[2:])

        # Find the specific date for this schedule based on the change window's context.
        # This assumes the change window correctly defines the month we are operating in.
        target_date = find_date_for_nth_weekday(
            year=change_window_start.year,
            month=change_window_start.month,
            nth_week=nth_week,
            weekday=weekday
        )

        # Combine the calculated date with the time from the group name
        scheduled_dt = datetime.combine(target_date, datetime.min.time(), tzinfo=tz)
        scheduled_dt = scheduled_dt.replace(hour=hour, minute=minute)
        
        return scheduled_dt

    except (ValueError, IndexError):
        # This will catch parsing errors (e.g., non-integer week number) or date calculation errors.
        return None

def tz_now(tz: ZoneInfo) -> datetime:
    return datetime.now(tz)

def wave_start_datetime(change_start: datetime, scheduled_hhmm: str) -> datetime:
    h, m = map(int, scheduled_hhmm.split(":"))
    candidate = change_start.replace(hour=h, minute=m, second=0, microsecond=0)
    if candidate < change_start:
        candidate = candidate + timedelta(days=1)
    return candidate

def sleep_until(target: datetime, tz_name: str, run_now: bool) -> None:
    if run_now:
        return
    while True:
        now = tz_now(ZoneInfo(tz_name))
        if now >= target:
            return
        time.sleep(min(30, max(3, int((target-now).total_seconds()))))