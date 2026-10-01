"""Feature code shared by training (build_training_data.py) and serving (dashboard.py).

Both sides import these functions, so an hourly mean, its coverage rule and the hour of
day are computed the same way when the model is trained and when it is used (no
training-serving skew).
"""
import csv
import io
import re
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from statistics import fmean
from zoneinfo import ZoneInfo

LOCAL_TZ = ZoneInfo("Europe/Amsterdam")
SITE_LABELS = ("hrl", "hrr", "vwd", "vwa")
FEATURES = ["total_intensity_veh_per_hr", "hour_of_day"]
ONE_HOUR = timedelta(hours=1)
NDW_KEY_RE = re.compile(r"^ndw/\d{4}-\d{2}-\d{2}/\d{2}-(hrl|hrr|vwd|vwa)\.csv$")
# A Luchtmeetnet value is the mean over the full hour, so a site's traffic mean only stands
# for that hour when its samples cover at least half of it: 10-minute cron gives 6 samples
# over 50 minutes, a window that is still filling (or hit by an outage) gives fewer.
MIN_SAMPLES = 3
MIN_SPAN = timedelta(minutes=30)


def to_utc(ts):
    """Timezone-aware UTC datetime from a datetime or an ISO 8601 string ('Z' allowed)."""
    if isinstance(ts, str):
        ts = datetime.fromisoformat(ts.strip().replace("Z", "+00:00"))
    if ts.tzinfo is None:
        raise ValueError(f"naive timestamp {ts!r}: a time zone is required")
    return ts.astimezone(timezone.utc)


def hour_of_day(ts):
    """Local (Europe/Amsterdam, DST aware) clock hour: traffic follows the local clock."""
    return to_utc(ts).astimezone(LOCAL_TZ).hour


def no2_window_label(measured_at):
    """Luchtmeetnet hour label an NDW sample belongs to.

    Luchtmeetnet stamps an hourly mean with the END of the hour (09:00 = 08:00-09:00
    UTC), so a sample measured at 08:30 belongs to the NO2 row timestamped 09:00.
    """
    return to_utc(measured_at).replace(minute=0, second=0, microsecond=0) + ONE_HOUR


def window_hour_of_day(label):
    """hour_of_day of the window START, i.e. the local hour the traffic was measured in.

    The model feature for an hourly traffic window. Training and serving both derive it
    from the label of the window whose traffic they use, never from the wall clock.
    """
    return hour_of_day(to_utc(label) - ONE_HOUR)


def ndw_key_site(key):
    """Site label for an hourly CSV key ndw/YYYY-MM-DD/HH-site.csv, else None."""
    match = NDW_KEY_RE.match(key)
    return match.group(1) if match else None


def parse_ndw_csv(data):
    """Rows (dicts) of one hourly NDW CSV file given as bytes."""
    return list(csv.DictReader(io.StringIO(data.decode("utf-8"))))


def _sample(row):
    """(measured_at, intensity) of a CSV row, or None if it is not a valid sample.

    A lane NDW marked dataError is written as 'None' in lane_flows, and ingestion then sums
    only the working lanes (an undercount) and flags the DB row
    (ingest_traffic.build_traffic_rows). Such samples are skipped here for the same reason.
    """
    try:
        lanes = [float(v) for v in row["lane_flows"].split(";")]  # 'None' or '' raise
        value = float(row["intensity_veh_per_hr"])
        measured_at = to_utc(row["measured_at"])
    except (AttributeError, KeyError, TypeError, ValueError):
        return None
    if not (value >= 0 and all(lane >= 0 for lane in lanes)):  # also drops NaN
        return None
    return measured_at, value


@dataclass(frozen=True)
class SiteWindow:
    """One site's valid samples in one hourly window."""
    mean: float  # veh/h, 1 decimal
    n: int
    first: datetime
    last: datetime

    @property
    def covered(self):
        return self.n >= MIN_SAMPLES and self.last - self.first >= MIN_SPAN

    def describe(self):
        return f"{self.n} samples over {int((self.last - self.first).total_seconds() // 60)} min"


def hourly_site_windows(rows):
    """{label: {site: SiteWindow}}; rows that are not valid samples are left out."""
    groups = defaultdict(lambda: defaultdict(list))
    for row in rows:
        sample = _sample(row)
        if sample and row.get("site") in SITE_LABELS:
            groups[no2_window_label(sample[0])][row["site"]].append(sample)
    return {label: {site: SiteWindow(round(fmean(v for _, v in samples), 1), len(samples),
                                     min(t for t, _ in samples), max(t for t, _ in samples))
                    for site, samples in sites.items()}
            for label, sites in groups.items()}


def total_intensity(site_means):
    """Sum of the four sites' hourly means, or None unless all four sites are present."""
    if any(site not in site_means for site in SITE_LABELS):
        return None
    return round(sum(site_means[site] for site in SITE_LABELS), 1)
