"""NDW traffic ingestion for the four A27 sites near Breda. Runs once and exits.

Why both the database and the bucket: the database gives fast, indexed queries for
serving the dashboard and the model. The bucket is a cheap, durable raw audit trail.
To retrain the model in six months, we re-parse the raw feed files, including with a
fixed parser if this one turns out to be wrong, which the database alone cannot give us
(it only holds what the parser of the day extracted).

Polling: cron samples every 10 minutes (`*/10 * * * *`) and training data is averaged
per hour. NDW values are 1-minute flow rates, so a single sample per hour is noisy.

Usage:
    python ingest_traffic.py                  one live sample
    python ingest_traffic.py --backfill DIR   load DIR/ndw/YYYY-MM-DD/HH-site.csv files
                                              written by tools/capture_ndw.py
"""
import argparse
import csv
import gzip
import io
import logging
import os
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import requests

from broker import messages_from_rows, publish_readings
from common import LocalStore, check_bad_threshold, fail_run, fetch, get_conn, get_s3, \
    log_event, record_run, setup_logging, write_rows

SOURCE = "NDW"
BACKFILL_SOURCE = "NDW_backfill"  # kept apart so a backfill does not trip the hourly threshold
MEASUREMENTS_URL = "https://opendata.ndw.nu/snelheden_en_intensiteiten_meetgegevens.xml.gz"
CONFIG_URL = "https://opendata.ndw.nu/snelheden_en_intensiteiten_configuratie_meetlocaties.xml.gz"
SITES = {
    "hrl": "RWS01_MONIBAS_0271hrl0063ra",  # A27 mainline, direction 1
    "hrr": "RWS01_MONIBAS_0271hrr0063ra",  # A27 mainline, direction 2
    "vwd": "RWS01_MONIBAS_0270vwd0063ra",  # entry slip road
    "vwa": "RWS01_MONIBAS_0270vwa0063ra",  # exit slip road
}
CSV_FIELDS = ["measured_at", "site", "site_id", "intensity_veh_per_hr", "speed_kmh",
              "lane_flows", "lane_speeds", "speed_invalid_count", "captured_at"]

TIME_RE = re.compile(r"<roa:measurementTimeDefault><roa:timeValue>([^<]+)</roa:timeValue>")
TYPE_RE = re.compile(r'xsi:type="roa:(TrafficFlow|TrafficSpeed)"')
VALUE_RE = re.compile(r"<com:(?:vehicleFlowRate|speed)>(-?[\d.]+)</com:")
DATA_ERROR = "<com:dataError>true</com:dataError>"

INSERT_SQL = """
    INSERT INTO sensor_readings (station_id, timestamp, component, value, is_flagged)
    VALUES %s
    ON CONFLICT (station_id, timestamp, component) DO NOTHING
"""


@dataclass
class SiteReading:
    label: str
    site_id: str
    measured_at: str   # as in the feed, e.g. 2026-10-01T08:31:00Z
    lane_flows: list   # veh/h per lane, None = missing or marked dataError by NDW
    lane_speeds: list  # km/h per lane, -1 = no valid speed, None = missing or dataError
    captured_at: str = ""

    @property
    def timestamp(self):
        return datetime.fromisoformat(self.measured_at.replace("Z", "+00:00"))

    @property
    def intensity_veh_per_hr(self):
        valid = [f for f in self.lane_flows if f is not None and f >= 0]
        return sum(valid) if valid else None

    @property
    def speed_kmh(self):
        valid = [s for s in self.lane_speeds if s is not None and s >= 0]
        return round(sum(valid) / len(valid), 1) if valid else None

    @property
    def speed_invalid_count(self):
        return sum(1 for s in self.lane_speeds if s is None or s < 0)

    def csv_row(self):
        return {"measured_at": self.measured_at, "site": self.label, "site_id": self.site_id,
                "intensity_veh_per_hr": self.intensity_veh_per_hr, "speed_kmh": self.speed_kmh,
                "lane_flows": ";".join(str(f) for f in self.lane_flows),
                "lane_speeds": ";".join(str(s) for s in self.lane_speeds),
                "speed_invalid_count": self.speed_invalid_count, "captured_at": self.captured_at}

    @classmethod
    def from_csv(cls, row):
        def lanes(text):
            return [None if v in ("", "None") else float(v) for v in text.split(";")] if text else []
        return cls(row["site"], row["site_id"], row["measured_at"],
                   lanes(row["lane_flows"]), lanes(row["lane_speeds"]), row["captured_at"])


def parse_sites(xml, captured_at=""):
    """SiteReading for each configured site found in the decompressed feed (bytes).

    Searches the bytes directly and decodes only the four site blocks: decoding the
    whole ~70 MB document would double peak memory on the 1 GB VM.
    """
    sites = []
    for label, site_id in SITES.items():
        start = xml.find(f'id="{site_id}"'.encode())
        block = "" if start == -1 else xml[start:xml.find(b"</roa:siteMeasurements>", start)].decode()
        measured = TIME_RE.search(block)
        if not measured:
            log_event(logging.WARNING, event="site_missing", source=SOURCE, site=label, location=site_id)
            continue
        flows, speeds = [], []
        # Each quantity is parsed on its own so a missing value cannot bleed into the next one.
        for part in block.split('<roa:physicalQuantity index="')[1:]:
            kind, value = TYPE_RE.search(part), VALUE_RE.search(part)
            if not kind:
                continue
            # NDW sends dataError=true with placeholder values (flow 0, speed 0.0 or -1).
            number = float(value.group(1)) if value and DATA_ERROR not in part else None
            (flows if kind.group(1) == "TrafficFlow" else speeds).append(number)
        sites.append(SiteReading(label, site_id, measured.group(1), flows, speeds, captured_at))
    return sites


def build_traffic_rows(sites):
    """Return (sensor_readings tuples, bad reading count) for parsed sites.

    The intensity row is always written (flagged if a lane flow is invalid). The speed
    row is skipped when any lane speed is invalid (-1): a sentinel, or a mean over only
    the working lanes, would corrupt the series.
    """
    rows, bad_count = [], 0
    for site in sites:
        ts = site.timestamp
        bad_flows = [f for f in site.lane_flows if f is None or f < 0]
        if bad_flows or not site.lane_flows:
            bad_count += 1
            log_event(logging.WARNING, event="DATA_QUALITY_ERROR", source=SOURCE,
                      location=site.site_id, field="intensity", value=None,
                      lanes_affected=len(bad_flows))
        rows.append((site.site_id, ts, "intensity", site.intensity_veh_per_hr,
                     bool(bad_flows) or not site.lane_flows))

        bad_speeds = [s for s in site.lane_speeds if s is None or s < 0]
        if bad_speeds or not site.lane_speeds:
            bad_count += 1
            log_event(logging.WARNING, event="DATA_QUALITY_ERROR", source=SOURCE,
                      location=site.site_id, field="speed",
                      value=bad_speeds[0] if bad_speeds else None,
                      lanes_affected=len(bad_speeds))
        else:
            rows.append((site.site_id, ts, "speed", site.speed_kmh, False))
    return rows, bad_count


def csv_key(site):
    """ndw/YYYY-MM-DD/HH-site.csv, from the measurement time (not the clock)."""
    return f"ndw/{site.timestamp:%Y-%m-%d}/{site.timestamp:%H}-{site.label}.csv"


def merge_csv(existing, rows):
    """Append rows whose measured_at is not yet in `existing` (CSV bytes or None).

    Returns (new CSV bytes, rows added). This lets cron sample several times per hour
    into one file per site and hour, and makes re-runs and backfills idempotent.
    """
    text = existing.decode("utf-8") if existing else ""
    seen = {r["measured_at"] for r in csv.DictReader(io.StringIO(text))}
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=CSV_FIELDS, extrasaction="ignore")
    if not text:
        writer.writeheader()
    elif not text.endswith("\n"):
        buf.write("\r\n")
    added = 0
    for row in rows:
        if row["measured_at"] not in seen:
            seen.add(row["measured_at"])
            writer.writerow(row)
            added += 1
    return (text + buf.getvalue()).encode("utf-8"), added


def save_csv(store, key, rows):
    # Read-modify-write is safe here because cron runs of this job never overlap.
    merged, added = merge_csv(store.get(key), rows)
    if added:
        store.put(key, merged)
    return added


def archive_config_once(store, day):
    """Store the site config gz once per day, as served: never decompressed or parsed
    (147 MB of XML would not fit next to everything else on the 1 GB VM)."""
    key = f"raw/ndw/config/{day:%Y-%m-%d}.xml.gz"
    try:
        if store.exists(key):
            return
        with fetch(CONFIG_URL, stream=True) as resp:
            resp.raw.decode_content = False
            store.put_stream(key, resp.raw)
        log_event(logging.INFO, event="config_archived", source=SOURCE, key=key, target=str(store))
    except Exception as exc:  # the measurements matter more; try again next run
        log_event(logging.WARNING, event="config_archive_failed", source=SOURCE, error=repr(exc))


def run_live(conn, store):
    captured = datetime.now(timezone.utc)
    try:
        raw = fetch(MEASUREMENTS_URL).content
    except requests.RequestException as exc:
        return fail_run(conn, SOURCE, "fetch_failed", exc)

    # Archive before parsing, so a feed this parser chokes on can still be re-parsed later.
    raw_key = f"raw/ndw/{captured:%Y-%m-%d}/{captured:%H%M%S}_meetgegevens.xml.gz"
    store.put(raw_key, raw)
    archive_config_once(store, captured)

    sites = parse_sites(gzip.decompress(raw), captured.isoformat(timespec="seconds"))
    if not sites:
        raise ValueError("none of the configured sites are in the feed")
    rows, bad_count = build_traffic_rows(sites)
    written = write_rows(conn, INSERT_SQL, rows)
    publish_readings(messages_from_rows(rows))  # one per site per metric; no-op unless REDIS_HOST

    for site in sites:
        added = save_csv(store, csv_key(site), [site.csv_row()])
        log_event(logging.INFO, event="fetch_success", source=SOURCE, station_id=site.site_id,
                  site=site.label, measured_at=site.measured_at,
                  intensity_veh_per_hr=site.intensity_veh_per_hr, speed_kmh=site.speed_kmh,
                  lane_speeds=site.lane_speeds, new_sample=bool(added))

    check_bad_threshold(conn, SOURCE, bad_count)
    record_run(conn, SOURCE, True, max(s.timestamp for s in sites), written, bad_count)
    return 0


def run_backfill(conn, store, src):
    files = sorted(Path(src).glob("ndw/*/*.csv"))
    if not files:
        raise FileNotFoundError(f"no CSV files under {Path(src) / 'ndw'}")
    db_rows, bad_count, appended = [], 0, 0
    for path in files:
        with path.open(newline="", encoding="utf-8") as fh:
            csv_rows = list(csv.DictReader(fh))
        rows, bad = build_traffic_rows([SiteReading.from_csv(r) for r in csv_rows])
        db_rows += rows
        bad_count += bad
        appended += save_csv(store, f"ndw/{path.parent.name}/{path.name}", csv_rows)

    written = write_rows(conn, INSERT_SQL, db_rows)
    last = max((r[1] for r in db_rows), default=None)
    log_event(logging.INFO, event="backfill_complete", source=SOURCE, files=len(files),
              csv_rows_appended=appended, db_rows_prepared=len(db_rows), rows_written=written,
              bad_data_count=bad_count, target=str(store))
    record_run(conn, BACKFILL_SOURCE, True, last, written, bad_count)
    return 0


def main(argv=None):
    parser = argparse.ArgumentParser(description="NDW traffic ingestion (runs once).")
    parser.add_argument("--backfill", metavar="DIR", type=Path,
                        help="load CSVs from DIR/ndw/ written by tools/capture_ndw.py")
    args = parser.parse_args(argv)
    setup_logging()
    source = BACKFILL_SOURCE if args.backfill else SOURCE
    conn = None
    try:
        conn = get_conn()
        store = get_s3() or LocalStore(os.getenv("OUTPUT_DIR", "out"))
        if args.backfill:
            return run_backfill(conn, store, args.backfill)
        return run_live(conn, store)
    except Exception as exc:  # DB, S3 or unexpected errors: log as JSON, exit non-zero
        return fail_run(conn, source, "run_failed", exc)
    finally:
        if conn is not None:
            conn.close()


if __name__ == "__main__":
    sys.exit(main())
