"""Shared helpers for the AirBreda ingestion containers (logging, DB, S3, HTTP)."""
import json
import logging
import math
import os
import shutil
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import boto3
import psycopg2
import requests
from boto3.s3.transfer import TransferConfig
from botocore.exceptions import ClientError
from psycopg2.extras import execute_values

BAD_DATA_LIMIT = 10  # bad readings per source per hour before BAD_DATA_THRESHOLD_EXCEEDED
_dry_run_announced = set()


def _json_default(obj):
    return obj.isoformat() if hasattr(obj, "isoformat") else str(obj)


class JsonFormatter(logging.Formatter):
    def format(self, record):
        message = record.getMessage()
        try:
            fields = json.loads(message)
        except ValueError:
            fields = None
        if not isinstance(fields, dict):
            fields = {"message": message}  # third-party library messages
        entry = {"ts": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                 "level": record.levelname, **fields}
        if record.exc_info:
            entry["exc"] = self.formatException(record.exc_info)
        return json.dumps(entry, default=_json_default)


def setup_logging(level=logging.INFO):
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    logging.basicConfig(level=level, handlers=[handler], force=True)
    for noisy in ("botocore", "boto3", "s3transfer", "urllib3"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


def log_event(level, **fields):
    # NaN is not valid JSON; null values must come out as null.
    fields = {k: None if isinstance(v, float) and math.isnan(v) else v for k, v in fields.items()}
    logging.log(level, json.dumps(fields, default=_json_default))


def _announce_dry_run(component, reason):
    if component not in _dry_run_announced:
        _dry_run_announced.add(component)
        log_event(logging.INFO, event="dry_run", component=component, reason=reason)


# --- HTTP -------------------------------------------------------------------------

def fetch(url, params=None, stream=False, timeout=30, retries=3, backoff=2.0):
    """GET with a timeout and up to `retries` retries (exponential backoff).

    Client errors other than 429 are not retried: asking again will not fix them.
    """
    for attempt in range(retries + 1):
        try:
            resp = requests.get(url, params=params, stream=stream, timeout=timeout,
                                headers={"User-Agent": "airbreda-ingest/1.0"})
            resp.raise_for_status()
            return resp
        except requests.RequestException as exc:
            status = getattr(exc.response, "status_code", None)
            if attempt == retries or (status and 400 <= status < 500 and status != 429):
                raise
            delay = backoff * 2 ** attempt
            log_event(logging.WARNING, event="fetch_retry", url=url, attempt=attempt + 1,
                      retry_in_s=delay, error=repr(exc))
            time.sleep(delay)


# --- Database ---------------------------------------------------------------------

def get_conn():
    """psycopg2 connection from env vars, or None (dry-run) when DB_HOST is unset."""
    host = os.getenv("DB_HOST")
    if not host:
        _announce_dry_run("database", "DB_HOST unset, nothing is written to PostgreSQL")
        return None
    return psycopg2.connect(
        host=host,
        port=int(os.getenv("DB_PORT", "5432")),
        dbname=os.getenv("DB_NAME", "airbreda"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        sslmode=os.getenv("DB_SSLMODE", "require"),
        connect_timeout=10,
    )


def write_rows(conn, sql, rows):
    """Run an `INSERT ... VALUES %s` for all rows in one statement; return rows affected."""
    if conn is None:
        if rows:
            log_event(logging.INFO, event="dry_run_db_skipped", rows_prepared=len(rows))
        return 0
    if not rows:
        return 0
    with conn, conn.cursor() as cur:
        # One page, so cur.rowcount covers every row (it only reports the last page).
        execute_values(cur, sql, rows, page_size=len(rows))
        return cur.rowcount


def record_run(conn, source, success, last_measurement, rows_written, bad_data_count, error=None):
    log_event(logging.INFO if success else logging.ERROR, event="run_recorded", source=source,
              success=success, last_measurement=last_measurement, rows_written=rows_written,
              bad_data_count=bad_data_count, error=error, persisted=conn is not None)
    if conn is None:
        return
    with conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO ingestion_runs (source, success, last_measurement, rows_written,"
            " bad_data_count, error) VALUES (%s, %s, %s, %s, %s, %s)",
            (source, success, last_measurement, rows_written, bad_data_count, error),
        )


def last_successful_measurement(conn, source):
    """last_measurement of the newest successful run of `source`; None on the first run
    and in dry-run. Lets a job tell the readings new to this run from the ones it re-fetches."""
    if conn is None:
        return None
    with conn, conn.cursor() as cur:
        cur.execute(
            "SELECT MAX(last_measurement) FROM ingestion_runs WHERE source = %s AND success",
            (source,),
        )
        return cur.fetchone()[0]


def recent_bad_count(conn, source):
    """Bad readings recorded for `source` over the last hour (0 in dry-run)."""
    if conn is None:
        return 0
    with conn, conn.cursor() as cur:
        cur.execute(
            "SELECT COALESCE(SUM(bad_data_count), 0) FROM ingestion_runs"
            " WHERE source = %s AND run_at > now() - interval '1 hour'",
            (source,),
        )
        return int(cur.fetchone()[0])


def check_bad_threshold(conn, source, bad_count):
    """Call before record_run, so this run's count is not added twice."""
    total = recent_bad_count(conn, source) + bad_count
    if total > BAD_DATA_LIMIT:
        log_event(logging.ERROR, event="BAD_DATA_THRESHOLD_EXCEEDED", source=source, count=total)


def fail_run(conn, source, event, exc, **fields):
    """Log the failure, record a failed run if the DB is reachable, return exit code 1."""
    log_event(logging.ERROR, event=event, source=source, **fields, error=repr(exc))
    try:
        if conn is not None:
            conn.rollback()
        record_run(conn, source, False, None, 0, 0, error=repr(exc))
    except Exception as db_exc:  # the DB itself may be what failed
        log_event(logging.ERROR, event="record_run_failed", source=source, error=repr(db_exc))
    return 1


# --- Object storage ---------------------------------------------------------------

class S3Store:
    """S3 bucket with the small get/put/exists interface the ingestion scripts need.

    The instance role needs s3:ListBucket as well as Get/PutObject: without it S3
    reports a missing key as 403 instead of 404, and get/exists would raise.
    """

    def __init__(self, client, bucket):
        self.client, self.bucket = client, bucket

    def __str__(self):
        return f"s3://{self.bucket}"

    def get(self, key):
        try:
            return self.client.get_object(Bucket=self.bucket, Key=key)["Body"].read()
        except ClientError as exc:
            if exc.response["Error"]["Code"] in ("NoSuchKey", "404"):
                return None
            raise

    def exists(self, key):
        try:
            self.client.head_object(Bucket=self.bucket, Key=key)
            return True
        except ClientError as exc:
            if exc.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
                return False
            raise

    def put(self, key, data):
        self.client.put_object(Bucket=self.bucket, Key=key, Body=data)

    def put_stream(self, key, fileobj):
        # Low concurrency keeps multipart buffers small on the 1 GB VM.
        self.client.upload_fileobj(fileobj, self.bucket, key,
                                   Config=TransferConfig(max_concurrency=2))


class LocalStore:
    """Same interface as S3Store, backed by a local directory (dry-run mode)."""

    def __init__(self, root):
        self.root = Path(root)

    def __str__(self):
        return str(self.root.resolve())

    def _path(self, key):
        path = self.root / key
        path.parent.mkdir(parents=True, exist_ok=True)
        return path

    def get(self, key):
        path = self.root / key
        return path.read_bytes() if path.exists() else None

    def exists(self, key):
        return (self.root / key).exists()

    def put(self, key, data):
        self._path(key).write_bytes(data)

    def put_stream(self, key, fileobj):
        # Write then rename, so an interrupted download never looks like a finished file.
        path = self._path(key)
        part = path.with_name(path.name + ".part")
        with part.open("wb") as fh:
            shutil.copyfileobj(fileobj, fh)
        part.replace(path)


def get_s3():
    """S3Store for S3_BUCKET using the default credential chain (instance role on EC2).

    Returns None (dry-run) when S3_BUCKET is unset.
    """
    bucket = os.getenv("S3_BUCKET")
    if not bucket:
        _announce_dry_run("s3", "S3_BUCKET unset, files go to OUTPUT_DIR instead")
        return None
    client = boto3.client("s3", region_name=os.getenv("AWS_REGION", "eu-north-1"))
    return S3Store(client, bucket)
