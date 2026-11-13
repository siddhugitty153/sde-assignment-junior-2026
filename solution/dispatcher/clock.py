import time
from datetime import datetime, time as dtime, timedelta, timezone

IST = timezone(timedelta(hours=5, minutes=30))
REGULATORY_START_STR = "09:00"
REGULATORY_END_STR = "21:00"
WINDOW_CLOSE_SAFETY_SEC = 45.0


def _parse_hm(hm_str: str) -> dtime:
    h, m = map(int, hm_str.split(":"))
    return dtime(h, m)


class SimClock:
    """Projects simulated provider time locally between /v1/clock syncs."""

    def __init__(self, sim_epoch: float = 0.0, real_epoch: float = 0.0, speed: float = 600.0):
        self.sim_epoch = sim_epoch
        self.real_epoch = real_epoch
        self.speed = speed

    def update_from_provider(self, payload: dict):
        self.sim_epoch = datetime.fromisoformat(payload["sim_epoch"]).timestamp()
        self.real_epoch = float(payload["real_epoch"])
        self.speed = float(payload["speed"])

    def now_ts(self) -> float:
        return self.sim_epoch + (time.time() - self.real_epoch) * self.speed

    def now_dt(self) -> datetime:
        return datetime.fromtimestamp(self.now_ts(), tz=IST)


def is_within_calling_window(
    sim_dt: datetime,
    window_start_str: str,
    window_end_str: str,
    allowed_days: list,
    safety_buffer_sec: float = WINDOW_CLOSE_SAFETY_SEC,
    regulatory_start_str: str = REGULATORY_START_STR,
    regulatory_end_str: str = REGULATORY_END_STR,
) -> bool:
    """
    Returns True iff both `sim_dt` and `sim_dt + safety_buffer_sec` fall inside:
      1. Campaign allowed days
      2. Regulatory window [regulatory_start, regulatory_end) IST
      3. Campaign window [window_start, window_end) IST
    """
    sim_dt_ist = sim_dt.astimezone(IST)
    sim_dt_buffered = sim_dt_ist + timedelta(seconds=safety_buffer_sec)

    reg_start = _parse_hm(regulatory_start_str)
    reg_end = _parse_hm(regulatory_end_str)
    win_start = _parse_hm(window_start_str)
    win_end = _parse_hm(window_end_str)
    eff_start = max(reg_start, win_start)
    eff_end = min(reg_end, win_end)
    if eff_start >= eff_end:
        return False

    allowed_set = {d.lower() if isinstance(d, str) else d for d in allowed_days}

    for dt in (sim_dt_ist, sim_dt_buffered):
        day_short = dt.strftime("%a").lower()
        day_full = dt.strftime("%A").lower()
        day_num = dt.weekday()
        if not ({day_short, day_full, day_num} & allowed_set):
            return False

        cur_t = dt.time()
        if not (eff_start <= cur_t < eff_end):
            return False

    return True


def next_window_open_ts(
    from_dt: datetime,
    window_start_str: str,
    window_end_str: str,
    allowed_days: list,
    regulatory_start_str: str = REGULATORY_START_STR,
    regulatory_end_str: str = REGULATORY_END_STR,
) -> float | None:
    """
    Returns the earliest simulated Unix timestamp >= from_dt when the effective
    campaign & regulatory calling window is open, searching up to 8 days ahead.
    """
    lt = from_dt.astimezone(IST)
    eff_start = max(_parse_hm(regulatory_start_str), _parse_hm(window_start_str))
    eff_end = min(_parse_hm(regulatory_end_str), _parse_hm(window_end_str))
    if eff_start >= eff_end:
        return None

    allowed_set = {d.lower() if isinstance(d, str) else d for d in allowed_days}

    for day_offset in range(0, 8):
        cand_day = lt + timedelta(days=day_offset)
        day_keys = {
            cand_day.strftime("%a").lower(),
            cand_day.strftime("%A").lower(),
            cand_day.weekday(),
        }
        if not (day_keys & allowed_set):
            continue

        s = cand_day.replace(
            hour=eff_start.hour, minute=eff_start.minute, second=0, microsecond=0
        )
        e = cand_day.replace(
            hour=eff_end.hour, minute=eff_end.minute, second=0, microsecond=0
        )
        if day_offset == 0:
            if lt < s:
                return s.timestamp()
            if lt < e:
                return lt.timestamp()
            continue
        return s.timestamp()

    return None

