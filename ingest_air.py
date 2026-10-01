"""Luchtmeetnet NO2 ingestion for station NL10240 (Breda). Runs once and exits.

CAP trade-off: the sensor network is an AP system. When a station has an outage the API
keeps answering (it stays available and partition tolerant) but serves stale or null
values instead of refusing, so consistency is what we give up. That is why this script
checks the data rather than trusting a 200 response.

Null values in production are kept and flagged (is_flagged = TRUE), never silently
dropped. A missing hour hurts the time series (resampling, lag features, model training)
more than a flagged value does, and the flag lets the dashboard and the model decide
what to do with it.

Polling interval: hourly, about 25 minutes after the hour (cron `25 * * * *`). The data
is hourly and published about 20 minutes after the hour ends, so per-minute polling would
add no new data and would burn the fair-use limit (100 requests / 5 min).
"""
import logging
import sys

import pandas as pd
import requests

from broker import messages_from_rows, publish_readings
from common import check_bad_threshold, fail_run, fetch, get_conn, last_successful_measurement, \
    log_event, record_run, setup_logging, write_rows

SOURCE = "Luchtmeetnet"
STATION = "NL10240"
COMPONENT = "NO2"
URL = f"https://api.luchtmeetnet.nl/open_api/stations/{STATION}/measurements"
PARAMS = {"formula": COMPONENT, "order_by": "timestamp_measured",
          "order_direction": "desc", "page": 1}
STALE_RUN = 3  # this many identical consecutive hourly values count as a frozen sensor

# DO UPDATE instead of the course's DO NOTHING: still idempotent (re-running a page
# changes nothing), and it picks up Luchtmeetnet's later validated revisions of a value.
# The flag is sticky while the value is unchanged: every run sees only the newest 50 hours,
# so the oldest members of a frozen run drop off the page first and the rest would look
# like a run shorter than STALE_RUN and be rewritten unflagged about 48 hours later
# (replayed in tests/test_data_quality.py). A revised value gets this run's fresh flag.
UPSERT_SQL = """
    INSERT INTO sensor_readings (station_id, timestamp, component, value, is_flagged)
    VALUES %s
    ON CONFLICT (station_id, timestamp, component)
    DO UPDATE SET
        value = EXCLUDED.value,
        is_flagged = CASE
            WHEN sensor_readings.value IS DISTINCT FROM EXCLUDED.value THEN EXCLUDED.is_flagged
            ELSE sensor_readings.is_flagged OR EXCLUDED.is_flagged
        END
"""


def fetch_no2():
    """Most recent page (50 hourly rows) as a DataFrame with component, value, timestamp."""
    data = fetch(URL, params=PARAMS).json().get("data", [])
    return pd.DataFrame(
        [{"component": r.get("formula"), "value": r.get("value"),
          "timestamp": r.get("timestamp_measured")} for r in data],
        columns=["component", "value", "timestamp"],
    )


def filter_no2_readings(df):
    """Keep NO2 rows, including rows whose value is null."""
    return df[df["component"] == COMPONENT].reset_index(drop=True)


def flag_bad_readings(df, since=None):
    """Return (df sorted by time with an is_flagged column, number of new flagged rows).

    A row is flagged when its value is null, or when the value is unchanged for
    STALE_RUN or more consecutive hourly timestamps (every row of that run is flagged).
    Flagged rows are kept.

    The rule runs over the whole page (the stale rule needs the neighbours) and every row
    is written with its flag, but only rows newer than `since` (the previous successful
    run's last measurement; None on the first run and in dry-run) are logged and counted.
    Otherwise one 11-hour outage would stay inside the 50-hour page for two days and be
    counted again on every hourly run, so ingestion_runs.bad_data_count and /health would
    describe the page, not the last hour, and BAD_DATA_THRESHOLD_EXCEEDED would fire hourly.
    """
    df = df.copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, format="ISO8601")
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df = df.sort_values("timestamp").reset_index(drop=True)

    continues_run = (df["value"].eq(df["value"].shift())
                     & df["timestamp"].diff().eq(pd.Timedelta(1, unit="h")))
    run_id = (~continues_run).cumsum()
    stale = df.groupby(run_id)["value"].transform("size") >= STALE_RUN
    df["is_flagged"] = df["value"].isna() | stale

    new = df["is_flagged"]
    if since is not None:
        since = pd.Timestamp(since)
        since = since.tz_localize("UTC") if since.tzinfo is None else since.tz_convert("UTC")
        new = new & (df["timestamp"] > since)
    for row in df[new].itertuples():
        log_event(logging.WARNING, event="DATA_QUALITY_ERROR", source=SOURCE, station_id=STATION,
                  field=COMPONENT, reason="stale_or_null", value=row.value,
                  timestamp=row.timestamp.isoformat())
    return df, int(new.sum())


def to_db_rows(df):
    """sensor_readings tuples. NaN becomes None (SQL NULL); psycopg2 would store NaN."""
    return [(STATION, row.timestamp.to_pydatetime(), COMPONENT,
             None if pd.isna(row.value) else float(row.value), bool(row.is_flagged))
            for row in df.itertuples()]


def run(conn):
    try:
        df = filter_no2_readings(fetch_no2())
        if df.empty:
            raise ValueError("response contained no NO2 rows")
    except (requests.RequestException, ValueError) as exc:  # ValueError covers bad JSON too
        return fail_run(conn, SOURCE, "fetch_failed", exc, station_id=STATION)

    df, bad_count = flag_bad_readings(df, since=last_successful_measurement(conn, SOURCE))
    rows = to_db_rows(df)
    written = write_rows(conn, UPSERT_SQL, rows)
    publish_readings(messages_from_rows(rows))  # no-op unless REDIS_HOST is set

    latest = df.iloc[-1]
    # The most recent value goes out as a JSON log line only (no plain print), so that
    # air.log stays one JSON object per line.
    log_event(logging.INFO, event="fetch_success", source=SOURCE, station_id=STATION,
              value=latest["value"], timestamp=latest["timestamp"].isoformat(),
              is_flagged=bool(latest["is_flagged"]))

    check_bad_threshold(conn, SOURCE, bad_count)
    record_run(conn, SOURCE, True, latest["timestamp"].to_pydatetime(), written, bad_count)
    return 0


def main():
    setup_logging()
    conn = None
    try:
        conn = get_conn()
        return run(conn)
    except Exception as exc:  # DB or unexpected errors: log as JSON, exit non-zero
        return fail_run(conn, SOURCE, "run_failed", exc, station_id=STATION)
    finally:
        if conn is not None:
            conn.close()


if __name__ == "__main__":
    sys.exit(main())
