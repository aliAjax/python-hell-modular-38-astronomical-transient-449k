import math
from datetime import datetime, timedelta, timezone


TWO_ARCMINUTES_DEG = 2.0 / 60.0
ONE_DAY = timedelta(days=1)


def parse_utc(value):
    """Parse an ISO-8601 timestamp and normalize it to UTC."""
    if isinstance(value, datetime):
        moment = value
    else:
        text = str(value or "").strip()
        if not text:
            return None
        if text.endswith(("Z", "z")):
            text = text[:-1] + "+00:00"
        try:
            moment = datetime.fromisoformat(text)
        except ValueError:
            return None
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=timezone.utc)
    return moment.astimezone(timezone.utc)


def angular_separation_deg(ra1, dec1, ra2, dec2):
    """Great-circle separation in degrees for ICRS-like degree coordinates."""
    ra1, dec1, ra2, dec2 = map(math.radians, (float(ra1), float(dec1), float(ra2), float(dec2)))
    dra = ra2 - ra1
    cos_angle = math.sin(dec1) * math.sin(dec2) + math.cos(dec1) * math.cos(dec2) * math.cos(dra)
    cos_angle = min(1.0, max(-1.0, cos_angle))
    return math.degrees(math.acos(cos_angle))


def reports_match(left, right, coordinate_limit_deg=TWO_ARCMINUTES_DEG, time_limit=ONE_DAY):
    """Match reports from different sources by position and observation time."""
    if left.get("source_id") == right.get("source_id"):
        return False
    separation = angular_separation_deg(left["ra"], left["dec"], right["ra"], right["dec"])
    if separation > coordinate_limit_deg:
        return False
    first_time = parse_utc(left.get("observed_at"))
    second_time = parse_utc(right.get("observed_at"))
    if first_time is None or second_time is None:
        return False
    return abs(first_time - second_time) <= time_limit
