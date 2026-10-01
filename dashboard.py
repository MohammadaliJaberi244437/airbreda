"""AirBreda dashboard: latest NO2, A27 traffic and the model's NO2 prediction (FastAPI).

Where each GET /site/{site_id} field comes from:
  no2_ug_m3, timestamp,       PostgreSQL sensor_readings, latest non-null NO2 at NL10240
  no2_is_flagged              (latest_no2). timestamp is the END of the measured hour;
                              no2_is_flagged is ingest_air's stale/suspect flag (training
                              leaves those hours out, the page warns about them).
  intensity_veh_per_hr,       S3 ndw/YYYY-MM-DD/HH-{site}.csv: mean of the site's newest
  traffic_window_end,         hourly window whose samples pass SiteWindow.covered, the same
  traffic_n_samples,          window and coverage rule as training (load_traffic, cached
  traffic_timestamp           60 s). traffic_window_end is the END of that hour, labelled
                              like timestamp; traffic_timestamp is its newest sample.
  total_intensity_veh_per_hr  sum of the four sites' means, only when all four use the same
                              window and that window is current (model_input).
  no2_ug_m3_predicted,        predict.predict(total, window_hour_of_day(window end)): the
  no2_exceedance_risk         local hour the traffic was measured in, as in training.
  no2_threshold_ug_m3         predict.THRESHOLD in use (NO2_THRESHOLD): risk 0.5 there.
  model_trained_at,           model_meta.json written by train_model.py (predict.model_meta).
  model_n_rows
  prediction_error            why the prediction fields are null; null otherwise.

A failing prediction degrades the response instead of failing it: the measured NO2 and
traffic are still valuable on their own, and a null prediction with prediction_error is
visible on the page and logged as an ERROR, not hidden behind a 500 that would also hide
the real readings. Only when PostgreSQL or S3 is down is there nothing real to show, and
then the answer is a 503 with the reason.

GET /history returns the last N hours of NO2 and of total traffic (hourly mean per site
summed over the four sites) straight from sensor_readings, for the page's chart.

GET /health reads ingestion_runs only: the ingestion containers run and exit, so that
table is the only record of whether they are working.
"""
import csv
import logging
import os
import threading
import time
from contextlib import closing
from datetime import datetime, timedelta, timezone

import boto3
import psycopg2
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError, CredentialRetrievalError, \
    NoCredentialsError, PartialCredentialsError
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse

from common import S3Store, get_conn, log_event, setup_logging
from features import MIN_SAMPLES, MIN_SPAN, NDW_SITE_IDS, SITE_LABELS, hourly_site_windows, ndw_key_site, \
    parse_ndw_csv, total_intensity, window_hour_of_day
from predict import THRESHOLD, model_meta, predict

STATION, COMPONENT = "NL10240", "NO2"
S3_CACHE_SECONDS = 60  # successes and failures alike
# Not common.get_s3's client: botocore's defaults (60 s connect and read timeouts, 5
# attempts) hold a worker thread for minutes per call while S3 is unreachable.
S3_CONFIG = Config(connect_timeout=3, read_timeout=5,
                   retries={"mode": "standard", "total_max_attempts": 2})
MAX_FILES_PER_SITE = 3
AIR_MAX_AGE = timedelta(hours=2)
TRAFFIC_MAX_AGE = timedelta(minutes=30)
HEALTH_SOURCES = {"luchtmeetnet": "Luchtmeetnet", "ndw": "NDW"}  # NDW_backfill left out

LATEST_NO2_SQL = """
    SELECT timestamp, value, is_flagged FROM sensor_readings
    WHERE station_id = %s AND component = %s AND value IS NOT NULL
    ORDER BY timestamp DESC LIMIT 1
"""
HEALTH_SQL = """
    SELECT source,
           MAX(run_at) FILTER (WHERE success),
           COALESCE(SUM(bad_data_count) FILTER (WHERE run_at > now() - interval '1 hour'), 0)
    FROM ingestion_runs WHERE source = ANY(%s) GROUP BY source
"""
HISTORY_NO2_SQL = """
    SELECT timestamp, value, is_flagged FROM sensor_readings
    WHERE station_id = %s AND component = %s AND timestamp > %s
    ORDER BY timestamp
"""
# Hourly mean intensity per NDW site, grouped by the Luchtmeetnet-style label of the hour
# (its END, in UTC), the same label features.no2_window_label gives a sample.
HISTORY_TRAFFIC_SQL = """
    SELECT date_trunc('hour', timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'UTC' + interval '1 hour',
           station_id, AVG(value), COUNT(*)
    FROM sensor_readings
    WHERE component = 'intensity' AND station_id = ANY(%s) AND timestamp > %s
    GROUP BY 1, 2
"""
MAX_HISTORY_HOURS = 168
NDW_SITE_LABELS = {site_id: label for label, site_id in NDW_SITE_IDS.items()}


class SourceUnavailable(Exception):
    """A backing store is down or not configured; `reason` is safe to show to clients."""

    def __init__(self, source, reason, detail=None):
        super().__init__(f"{source}: {reason}")
        self.source, self.reason, self.detail = source, reason, detail


def _configure_logging():
    """JSON logging for every line, installed at import time: uvicorn imports this module
    before it logs "Started server process", so configuring in the lifespan hook would leave
    those first lines in plain text. uvicorn's own plain-text handlers are dropped (its
    records propagate to the JSON root handler) and its access log is replaced by
    log_requests below."""
    setup_logging()
    for name in ("uvicorn", "uvicorn.error"):
        logging.getLogger(name).handlers.clear()
        logging.getLogger(name).propagate = True
    logging.getLogger("uvicorn.access").disabled = True


_configure_logging()
app = FastAPI(title="AirBreda dashboard")


@app.middleware("http")
async def log_requests(request: Request, call_next):
    start, status = time.perf_counter(), 500
    try:
        response = await call_next(request)
        status = response.status_code
        return response
    finally:
        log_event(logging.INFO if status < 500 else logging.WARNING, event="request",
                  method=request.method, path=request.url.path, status=status,
                  ms=round((time.perf_counter() - start) * 1000, 1))


def _log_unavailable(path, exc):
    log_event(logging.ERROR, event="source_unavailable", path=path,
              source=exc.source, reason=exc.reason, error=exc.detail)


@app.exception_handler(SourceUnavailable)
async def source_unavailable(request: Request, exc: SourceUnavailable):
    _log_unavailable(request.url.path, exc)
    return JSONResponse(status_code=503, content={
        "error": "service unavailable", "source": exc.source, "reason": exc.reason})


@app.exception_handler(Exception)
async def unhandled(request: Request, exc: Exception):
    log_event(logging.ERROR, event="unhandled_error", path=request.url.path, error=repr(exc))
    return JSONResponse(status_code=500, content={"error": "internal server error"})


def _iso(ts):
    return ts.astimezone(timezone.utc).isoformat() if ts else None


# --- Data sources (tests replace these) --------------------------------------------

def _query(sql, params):
    """All rows of one read-only query, on a connection that is always closed."""
    try:
        conn = get_conn()
        if conn is None:
            raise SourceUnavailable("database", "database not configured (DB_HOST unset)")
        with closing(conn), conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchall()
    except psycopg2.Error as exc:
        raise SourceUnavailable("database", "database unreachable", repr(exc)) from exc


def latest_no2():
    """(timestamp, value, is_flagged) of the newest non-null NO2 at NL10240, or None."""
    rows = _query(LATEST_NO2_SQL, (STATION, COMPONENT))
    return rows[0] if rows else None


def ingestion_summary():
    """{source: (last successful run_at or None, bad_data_count over the last hour)}."""
    rows = _query(HEALTH_SQL, (list(HEALTH_SOURCES.values()),))
    return {source: (last_ok, int(bad)) for source, last_ok, bad in rows}


def load_traffic(store, now):
    """{site: {"intensity", "measured_at", "window_end", "n_samples"}}.

    Each site's newest hourly window that passes SiteWindow.covered, searched in its
    MAX_FILES_PER_SITE newest CSVs. While the current hour has too few samples (the first
    ~40 minutes of every hour) that is the hour that just ended, never a lone 1-minute
    sample. Lists today's prefix (UTC, as the keys are) and yesterday's when today holds
    fewer files than that for a site.
    """
    keys = {}
    for day in (now, now - timedelta(days=1)):
        if all(len(keys.get(s, ())) >= MAX_FILES_PER_SITE for s in SITE_LABELS):
            break
        listing = store.client.list_objects_v2(Bucket=store.bucket, Prefix=f"ndw/{day:%Y-%m-%d}/")
        for obj in listing.get("Contents", []):
            if site := ndw_key_site(obj["Key"]):
                keys.setdefault(site, []).append(obj["Key"])
    traffic = {}
    for site, site_keys in keys.items():
        for key in sorted(site_keys, reverse=True)[:MAX_FILES_PER_SITE]:
            try:
                windows = hourly_site_windows(parse_ndw_csv(store.get(key) or b""))
            except (ValueError, csv.Error) as exc:  # one corrupt file must not hide the rest
                log_event(logging.WARNING, event="traffic_file_unreadable", key=key,
                          error=repr(exc))
                continue
            covered = [(label, w[site]) for label, w in windows.items()
                       if site in w and w[site].covered]
            if covered:
                label, window = max(covered, key=lambda item: item[0])
                traffic[site] = {"intensity": window.mean, "measured_at": window.last,
                                 "window_end": label, "n_samples": window.n}
                break
    return traffic


def _open_store():
    bucket = os.getenv("S3_BUCKET")
    if not bucket:
        return None
    return S3Store(boto3.client("s3", region_name=os.getenv("AWS_REGION", "eu-north-1"),
                                config=S3_CONFIG), bucket)


def _s3_reason(exc):
    """Client-safe reason for a failed S3 round."""
    code = exc.response.get("Error", {}).get("Code", "") if isinstance(exc, ClientError) else ""
    # botocore raises a plain RuntimeError when env credentials with an expiry run out.
    if "expired" in f"{code} {exc}".lower():
        return "S3 credentials expired"
    if isinstance(exc, (NoCredentialsError, PartialCredentialsError, CredentialRetrievalError)):
        return "S3 credentials missing or incomplete"
    if code in ("AccessDenied", "InvalidAccessKeyId", "SignatureDoesNotMatch", "403"):
        return "S3 access denied"
    if isinstance(exc, BotoCoreError):
        return "S3 unreachable"
    return f"S3 request failed ({code})" if code else "S3 request failed"


_store = None
_traffic_cache = {"at": None, "value": None, "error": None}
_traffic_lock = threading.Lock()


def traffic_snapshot():
    """load_traffic, cached for S3_CACHE_SECONDS. The lock makes the page's four parallel
    /site calls share one S3 round, and a cached failure keeps an S3 outage from costing
    every request its own round of timeouts."""
    global _store
    with _traffic_lock:
        cache = _traffic_cache
        if cache["at"] is None or time.monotonic() - cache["at"] >= S3_CACHE_SECONDS:
            value = error = None
            try:
                if _store is None:
                    _store = _open_store()
                if _store is None:
                    raise SourceUnavailable("s3", "S3 not configured (S3_BUCKET unset)")
                value = load_traffic(_store, datetime.now(timezone.utc))
            except SourceUnavailable as exc:
                error = exc
            except Exception as exc:
                error = SourceUnavailable("s3", _s3_reason(exc), repr(exc))
            cache.update(at=time.monotonic(), value=value, error=error)
        if cache["error"] is not None:
            raise cache["error"].with_traceback(None)
        return cache["value"]


def model_input(traffic, now):
    """(total intensity, window label, None) if the traffic can feed the model, else
    (None, None, reason). A training row sums the four sites over ONE hourly window, so
    the sites' windows must match, and be current: the hour that just ended or the one
    that is filling now."""
    if missing := [s for s in SITE_LABELS if s not in traffic]:
        return None, None, f"no usable hourly traffic window for site(s): {', '.join(missing)}"
    ends = {t["window_end"] for t in traffic.values()}
    if len(ends) > 1:
        return None, None, "traffic windows not aligned: " + ", ".join(
            f"{s} hour ending {_iso(traffic[s]['window_end'])}" for s in SITE_LABELS)
    label = ends.pop()
    if label < now.replace(minute=0, second=0, microsecond=0):
        return None, None, (f"traffic stale: the newest usable hourly window ended "
                            f"{_iso(label)}, before the current hour")
    return total_intensity({s: t["intensity"] for s, t in traffic.items()}), label, None


# --- Endpoints ---------------------------------------------------------------------

@app.get("/site/{site_id}")
def site(site_id: str):
    if site_id not in SITE_LABELS:
        return JSONResponse(status_code=404, content={
            "error": f"unknown site '{site_id}'", "known_sites": list(SITE_LABELS)})
    no2 = latest_no2()
    traffic = traffic_snapshot()
    total, label, error = model_input(traffic, datetime.now(timezone.utc))
    predicted = risk = None
    if error is None:
        try:
            result = predict(total, window_hour_of_day(label))
            predicted, risk = result["no2_ug_m3_predicted"], result["no2_exceedance_risk"]
        except Exception as exc:  # degrade, do not fail: see the module docstring
            # Only the exception type goes to the client; the message can hold container
            # paths or library internals, so it stays in the server log (like the 503 path).
            error = f"prediction failed ({type(exc).__name__}); see server log"
            log_event(logging.ERROR, event="prediction_failed", site_id=site_id,
                      total_intensity_veh_per_hr=total, error=repr(exc))
    own = traffic.get(site_id, {})
    meta = model_meta()
    return {
        "site_id": site_id,
        "no2_ug_m3": no2[1] if no2 else None,
        "intensity_veh_per_hr": own.get("intensity"),
        "no2_exceedance_risk": risk,
        "timestamp": _iso(no2[0]) if no2 else None,
        "no2_is_flagged": bool(no2[2]) if no2 else None,
        "no2_ug_m3_predicted": predicted,
        "no2_threshold_ug_m3": THRESHOLD,
        "total_intensity_veh_per_hr": total,
        "traffic_window_end": _iso(own.get("window_end")),
        "traffic_n_samples": own.get("n_samples"),
        "traffic_timestamp": _iso(own.get("measured_at")),
        "model_trained_at": meta.get("trained_at"),
        "model_n_rows": meta.get("n_rows"),
        "prediction_error": error,
    }


def health_status(now, last_air, last_traffic):
    """'ok' unless Luchtmeetnet had no success in 2 h or NDW none in 30 min."""
    def fresh(last, max_age):
        return last is not None and now - last <= max_age
    ok = fresh(last_air, AIR_MAX_AGE) and fresh(last_traffic, TRAFFIC_MAX_AGE)
    return "ok" if ok else "degraded"


@app.get("/health")
def health():
    now = datetime.now(timezone.utc)
    try:
        summary = ingestion_summary()
    except SourceUnavailable as exc:  # same body shape, so monitors can still read status
        _log_unavailable("/health", exc)
        unknown = {"last_successful_fetch": None, "bad_data_count": None}
        return JSONResponse(status_code=503, content={
            "status": "degraded", **{key: unknown for key in HEALTH_SOURCES},
            "checked_at": _iso(now), "error": exc.reason})
    sources = {key: summary.get(source, (None, 0)) for key, source in HEALTH_SOURCES.items()}
    return {
        "status": health_status(now, sources["luchtmeetnet"][0], sources["ndw"][0]),
        **{key: {"last_successful_fetch": _iso(last), "bad_data_count": bad}
           for key, (last, bad) in sources.items()},
        "checked_at": _iso(now),
    }


def traffic_history(rows):
    """[{window_end, total_intensity_veh_per_hr, sites, min_samples}], oldest first, for the
    hours in which all four sites have at least MIN_SAMPLES samples. rows are
    (hour label, station_id, mean intensity, sample count) from HISTORY_TRAFFIC_SQL."""
    per_hour = {}
    for label, station_id, mean, n in rows:
        if site := NDW_SITE_LABELS.get(station_id):
            per_hour.setdefault(label, {})[site] = (round(float(mean), 1), int(n))
    out = []
    for label in sorted(per_hour):
        sites = per_hour[label]
        if all(s in sites and sites[s][1] >= MIN_SAMPLES for s in SITE_LABELS):
            out.append({"window_end": _iso(label),
                        "total_intensity_veh_per_hr": total_intensity({s: sites[s][0] for s in SITE_LABELS}),
                        "sites": {s: sites[s][0] for s in SITE_LABELS},
                        "min_samples": min(sites[s][1] for s in SITE_LABELS)})
    return out


@app.get("/history")
def history(hours: int = 24):
    """Hourly NO2 and total traffic for the page's 24-hour chart.

    A traffic hour counts when all four sites have MIN_SAMPLES samples in it: a looser cousin
    of the training rule, which also needs a 30-minute span that this grouped query cannot
    see. The chart is descriptive; the model input (GET /site) keeps the strict rule."""
    hours = max(1, min(hours, MAX_HISTORY_HOURS))
    since = datetime.now(timezone.utc) - timedelta(hours=hours)
    no2 = [{"timestamp": _iso(ts), "value": value, "is_flagged": bool(flagged)}
           for ts, value, flagged in _query(HISTORY_NO2_SQL, (STATION, COMPONENT, since))]
    traffic = traffic_history(_query(HISTORY_TRAFFIC_SQL, (list(NDW_SITE_LABELS), since)))
    failed, error = 0, None
    for item in traffic:  # the model's prediction for each past hour, next to what was measured
        try:
            result = predict(item["total_intensity_veh_per_hr"], window_hour_of_day(item["window_end"]))
            item["no2_ug_m3_predicted"] = result["no2_ug_m3_predicted"]
            item["no2_exceedance_risk"] = result["no2_exceedance_risk"]
        except Exception as exc:  # degrade like /site does: the measured hours are still served
            item["no2_ug_m3_predicted"] = item["no2_exceedance_risk"] = None
            failed, error = failed + 1, repr(exc)
    if failed:
        log_event(logging.ERROR, event="history_prediction_failed", hours=failed, error=error)
    return {"hours": hours, "since": _iso(since), "threshold_ug_m3": THRESHOLD,
            "no2": no2, "traffic": traffic}


@app.get("/", response_class=HTMLResponse)
def index():
    return PAGE


PAGE = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>AirBreda: NO2 and A27 traffic</title>
<style>
:root { color-scheme: light; --page:#f9f9f7; --surface:#fcfcfb; --ink:#0b0b0b; --ink-2:#52514e; --muted:#6e6c66;
  --grid:#e1e0d9; --axis:#c3c2b7; --border:rgba(11,11,11,0.10); --blue:#2a78d6; --blue-track:#cde2fb; --orange:#eb6834; --aqua:#1baf7a;
  --s1:#1c5cab; --s2:#2a78d6; --s3:#5598e7; --s4:#86b6ef; --good:#006300; --good-fill:#0ca30c; --warn-fill:#fab219;
  --critical:#d03b3b; --warn-bg:#fff4d6; --warn-ink:#5e4300; --err-bg:#fde6e6; --err-ink:#7f1717; --focus:#2a78d6; }
@media (prefers-color-scheme: dark) { :root { color-scheme: dark; --page:#0d0d0d; --surface:#1a1a19; --ink:#ffffff; --ink-2:#c3c2b7;
  --muted:#9a988f; --grid:#2c2c2a; --axis:#383835; --border:rgba(255,255,255,0.10); --blue:#3987e5; --blue-track:#184f95; --orange:#d95926; --aqua:#199e70;
  --s1:#2a78d6; --s2:#3987e5; --s3:#6da7ec; --s4:#9ec5f4; --good:#0ca30c; --warn-bg:#3a2f12; --warn-ink:#f3d58a; --err-bg:#3d1c1c; --err-ink:#f4b4b4; --focus:#6aa0ff; } }
* { box-sizing:border-box; }
body { margin:0; background:var(--page); color:var(--ink); font:16px/1.5 system-ui, -apple-system, "Segoe UI", sans-serif; }
main { max-width:1120px; margin:0 auto; padding:28px 20px 48px; }
a { color:var(--blue); }
:focus-visible { outline:2px solid var(--focus); outline-offset:2px; border-radius:4px; }
.top { display:flex; justify-content:space-between; align-items:flex-start; gap:24px; flex-wrap:wrap; margin-bottom:20px; }
h1 { font-size:24px; line-height:1.2; margin:0 0 6px; letter-spacing:-0.01em; }
h2 { font-size:17px; margin:0; }
.sub { margin:0; color:var(--ink-2); max-width:62ch; }
.meta { color:var(--muted); font-size:14px; }
.status { display:flex; flex-direction:column; align-items:flex-end; gap:6px; }
.pill { display:inline-flex; align-items:center; gap:8px; padding:6px 12px; border-radius:999px; border:1px solid var(--border);
  background:var(--surface); font-size:14px; font-weight:500; }
.pill svg { width:16px; height:16px; flex:none; }
.pill.ok svg { color:var(--good-fill); } .pill.degraded svg { color:var(--critical); } .pill.unknown svg { color:var(--muted); }
#banners:empty { display:none; }
.banner { border-radius:8px; padding:10px 14px; margin:0 0 10px; font-weight:500; font-size:15px; }
.warn { background:var(--warn-bg); color:var(--warn-ink); } .err { background:var(--err-bg); color:var(--err-ink); }
.card { background:var(--surface); border:1px solid var(--border); border-radius:12px; padding:18px 20px; }
.kpis { display:grid; grid-template-columns:repeat(auto-fit, minmax(260px, 1fr)); gap:14px; margin:0 0 14px; }
.label { color:var(--ink-2); font-size:14px; }
.value { font-size:44px; font-weight:600; line-height:1.1; margin:4px 0 2px; letter-spacing:-0.02em; }
.unit { font-size:16px; color:var(--muted); font-weight:400; margin-left:4px; letter-spacing:0; }
.meter { position:relative; height:8px; border-radius:4px; background:var(--blue-track); margin:12px 0 8px; overflow:visible; }
.meter > span { display:block; height:100%; border-radius:4px; background:var(--blue); width:0; transition:width 300ms ease; }
.meter .tick { position:absolute; top:-4px; width:2px; height:16px; background:var(--ink-2); }
.meter .tick-label { position:absolute; top:14px; transform:translateX(-50%); font-size:12px; color:var(--muted); white-space:nowrap; }
.kpi-foot { color:var(--muted); font-size:14px; margin-top:22px; }
.shares { display:flex; gap:2px; height:8px; margin:12px 0 8px; border-radius:4px; overflow:hidden; }
.shares > span { display:block; height:100%; min-width:2px; transition:flex-basis 300ms ease; }
.card-head { display:flex; justify-content:space-between; align-items:baseline; gap:16px; flex-wrap:wrap; margin-bottom:6px; }
.legend { display:flex; gap:18px; flex-wrap:wrap; font-size:14px; color:var(--ink-2); }
.legend i { display:inline-block; width:14px; height:3px; vertical-align:middle; margin-right:6px; border-radius:2px; }
.legend i.col { height:10px; width:10px; border-radius:2px; }
.chart { position:relative; margin-top:6px; }
.plot { overflow-x:auto; overflow-y:hidden; padding-bottom:4px; }
.chart svg { display:block; width:100%; min-width:640px; height:auto; overflow:visible; }
.chart text { font:12px system-ui, -apple-system, "Segoe UI", sans-serif; fill:var(--muted); }
.chart .lbl { fill:var(--ink-2); font-weight:600; }
.chart .ttl { fill:var(--ink-2); font-size:13px; font-weight:600; }
.chart line.grid { stroke:var(--grid); stroke-width:1; } .chart line.base { stroke:var(--axis); stroke-width:1; }
.chart line.ref { stroke:var(--ink-2); stroke-width:1; stroke-dasharray:0; opacity:0.7; }
.chart .series { stroke:var(--blue); stroke-width:2; fill:none; stroke-linejoin:round; stroke-linecap:round; }
.chart .series.pred { stroke:var(--aqua); stroke-dasharray:7 6; } .chart .dot.pred { fill:var(--aqua); }
.legend i.dash { height:0; border-top:3px dashed var(--aqua); background:none; border-radius:0; }
.chart .wash { fill:var(--blue); opacity:0.10; }
.chart .dot { fill:var(--blue); stroke:var(--surface); stroke-width:2; } .chart .dot.flag { fill:var(--surface); stroke:var(--blue); }
.chart .col { fill:var(--orange); } .chart .col.hot { opacity:0.75; }
.chart .hair { stroke:var(--ink-2); stroke-width:1; opacity:0; } .chart.active .hair { opacity:0.6; }
.chart .hit { fill:transparent; cursor:crosshair; }
.tip { position:absolute; pointer-events:none; background:var(--surface); border:1px solid var(--border); border-radius:8px;
  padding:8px 12px; font-size:14px; box-shadow:0 4px 16px rgba(0,0,0,0.12); opacity:0; transition:opacity 120ms; min-width:190px; }
.tip.show { opacity:1; } .tip .t { color:var(--muted); font-size:13px; margin-bottom:4px; }
.tip .row { display:flex; justify-content:space-between; gap:16px; } .tip b { font-variant-numeric:tabular-nums; }
.tip i { display:inline-block; width:12px; height:3px; vertical-align:middle; margin-right:6px; border-radius:2px; }
details { margin-top:8px; } summary { cursor:pointer; color:var(--blue); font-size:14px; }
.scroll { overflow-x:auto; margin-top:12px; }
table { width:100%; border-collapse:collapse; font-size:15px; }
th, td { text-align:left; padding:10px 12px; border-bottom:1px solid var(--grid); white-space:nowrap; vertical-align:middle; }
td { font-variant-numeric:tabular-nums; } th { font-size:13px; color:var(--muted); font-weight:600; }
tr:last-child td { border-bottom:none; } .num { text-align:right; }
.site { display:flex; align-items:center; gap:10px; } .site i { width:10px; height:10px; border-radius:2px; flex:none; }
.site small { display:block; color:var(--muted); font-size:13px; }
.bar { display:inline-block; width:120px; height:6px; background:var(--grid); border-radius:3px; margin-left:10px; vertical-align:middle; }
.bar span { display:block; height:100%; background:var(--blue); border-radius:3px; transition:width 300ms ease; }
.risk { display:inline-flex; align-items:center; gap:10px; } .risk .m { width:72px; height:6px; background:var(--grid); border-radius:3px; }
.risk .m span { display:block; height:100%; border-radius:3px; } .risk small { color:var(--muted); }
.notes { margin-top:14px; color:var(--muted); font-size:14px; } .notes p { margin:6px 0; }
.links { display:flex; gap:16px; flex-wrap:wrap; margin-top:10px; font-size:14px; }
@media (max-width:640px) { .value { font-size:38px; } .status { align-items:flex-start; } main { padding:20px 16px 40px; } .bar { width:72px; } }
@media (prefers-reduced-motion: reduce) { *, *::before, *::after { transition:none !important; } }
</style>
</head>
<body>
<main>
<header class="top">
  <div>
    <h1>AirBreda: NO<sub>2</sub> and A27 traffic</h1>
    <p class="sub">Hourly nitrogen dioxide at Luchtmeetnet station NL10240 (Breda-Tilburgseweg) and vehicle intensity at four NDW measuring sites on the A27 interchange, with the model's prediction for the current hour.</p>
  </div>
  <div class="status">
    <span class="pill unknown" id="health" title="Ingestion status from /health"><svg viewBox="0 0 24 24" aria-hidden="true"><circle cx="12" cy="12" r="9" fill="none" stroke="currentColor" stroke-width="2"/></svg><span id="health-text">Checking ingestion</span></span>
    <span class="meta" id="updated">Loading</span>
  </div>
</header>
<div id="banners" aria-live="polite"></div>

<section class="kpis" aria-label="Current readings">
  <div class="card">
    <div class="label">Measured NO<sub>2</sub></div>
    <div class="value" id="no2">-</div>
    <div class="meter" role="meter" aria-label="Measured NO2 against the __THRESHOLD__ microgram threshold" aria-valuemin="0" aria-valuemax="80" aria-valuenow="0" id="no2-meter"><span></span><i class="tick" id="no2-tick"></i><span class="tick-label" id="no2-tick-label">__THRESHOLD__ &micro;g/m&sup3;</span></div>
    <div class="kpi-foot" id="no2-time">Loading</div>
  </div>
  <div class="card">
    <div class="label">Predicted NO<sub>2</sub> for this hour</div>
    <div class="value" id="pred">-</div>
    <div class="meter" role="meter" aria-label="Exceedance risk from 0 to 1" aria-valuemin="0" aria-valuemax="1" aria-valuenow="0" id="risk-meter"><span></span></div>
    <div class="kpi-foot" id="risk">Loading</div>
  </div>
  <div class="card">
    <div class="label">Total traffic, four sites</div>
    <div class="value" id="total">-</div>
    <div class="shares" id="shares" aria-hidden="true"></div>
    <div class="kpi-foot" id="traffic-time">Loading</div>
  </div>
</section>

<section class="card" aria-label="Last 24 hours">
  <div class="card-head">
    <div><h2>Last 24 hours</h2><div class="meta">One point per hour. Hover or use the arrow keys for values.</div></div>
    <div class="legend"><span><i style="background:var(--blue)"></i>NO<sub>2</sub> (&micro;g/m&sup3;)</span><span><i class="dash"></i>Predicted NO<sub>2</sub> (model)</span><span><i class="col" style="background:var(--orange)"></i>Total traffic (veh/h)</span></div>
  </div>
  <div class="chart" id="chart" tabindex="0" aria-label="NO2 and traffic over the last 24 hours; the table below holds the same values">
    <div class="plot"><svg id="svg" viewBox="0 0 1000 420" role="img" aria-hidden="true"></svg></div>
    <div class="tip" id="tip"></div>
  </div>
  <details><summary>Show the 24 hours as a table</summary>
    <div class="scroll"><table><thead><tr><th>Hour (local)</th><th class="num">NO<sub>2</sub> (&micro;g/m&sup3;)</th><th class="num">Predicted NO<sub>2</sub></th><th class="num">Total traffic (veh/h)</th></tr></thead><tbody id="hist-rows"></tbody></table></div>
  </details>
</section>

<section class="card" style="margin-top:14px" aria-label="Per site">
  <div class="card-head"><div><h2>The four A27 sites</h2><div class="meta">Hourly mean intensity per site. The prediction uses the total of all four, so it is the same for every site.</div></div></div>
  <div class="scroll"><table>
  <thead><tr><th>Site</th><th class="num">Traffic (veh/h)</th><th>Hourly window (local)</th><th class="num">Predicted NO<sub>2</sub></th><th>Exceedance risk</th></tr></thead>
  <tbody id="rows"></tbody>
  </table></div>
</section>

<div class="notes">
  <p id="model">Model: loading.</p>
  <p>Traffic is the mean of a site's samples in its newest clock hour with at least __MIN_SAMPLES__ samples spanning __MIN_SPAN__ minutes,
  the rule the training data uses; until the current hour has that many, the hour that just ended is shown. Exceedance risk is a 0-1 score,
  not a calibrated probability: a sigmoid of predicted NO<sub>2</sub> minus __THRESHOLD__ &micro;g/m&sup3; (0.5 at __THRESHOLD__; set by
  NO2_THRESHOLD, default 40, the EU annual limit value, used here as an hourly reference). Hours are labelled by their end, as Luchtmeetnet does.</p>
  <div class="links"><a href="/site/hrl">/site/hrl</a><a href="/health">/health</a><a href="/history?hours=24">/history</a><a href="https://mohammadalijaberi244437.github.io/airbreda/">Architecture document</a></div>
</div>
</main>
<script>
const SITES = ["hrl", "hrr", "vwd", "vwa"];
const NAMES = {hrl: "A27 mainline, direction 1", hrr: "A27 mainline, direction 2",
               vwd: "Entry slip road (leaving Breda)", vwa: "Exit slip road (entering Breda)"};
const SWATCH = {hrl: "var(--s1)", hrr: "var(--s2)", vwd: "var(--s3)", vwa: "var(--s4)"};
const AIR_MAX_MIN = 120, FETCH_TIMEOUT_MS = 15000, HOUR_MS = 3600000;
const $ = id => document.getElementById(id);
const has = v => v !== null && v !== undefined;
const num = (v, d) => has(v) ? Number(v).toLocaleString("en-GB", {minimumFractionDigits: d, maximumFractionDigits: d}) : "n/a";
const ageMin = iso => (Date.now() - new Date(iso).getTime()) / 60000;
const TZ = {timeZone: "Europe/Amsterdam"};
const local = iso => has(iso) ? new Date(iso).toLocaleString("en-GB", {...TZ, day: "2-digit", month: "short", hour: "2-digit", minute: "2-digit"}) : "n/a";
const clock = iso => new Date(iso).toLocaleTimeString("en-GB", {...TZ, hour: "2-digit", minute: "2-digit"});
// Hours are labelled by their END, like Luchtmeetnet's: show them as start-end.
const windowText = end => has(end) ? clock(new Date(new Date(end).getTime() - HOUR_MS).toISOString()) + "-" + clock(end) : "n/a";
const hourStart = () => { const d = new Date(); d.setUTCMinutes(0, 0, 0); return d; };
const SVG = "http://www.w3.org/2000/svg";

function el(tag, attrs, text) {
  const node = document.createElement(tag);
  for (const [k, v] of Object.entries(attrs || {})) k === "style" ? node.style.cssText = v : node.setAttribute(k, v);
  if (has(text)) node.textContent = text;
  return node;
}
function svgEl(tag, attrs, text) {
  const node = document.createElementNS(SVG, tag);
  for (const [k, v] of Object.entries(attrs || {})) node.setAttribute(k, v);
  if (has(text)) node.textContent = text;
  return node;
}
function setValue(id, value, digits, unit) {
  const box = $(id);
  box.replaceChildren(document.createTextNode(num(value, digits)), el("span", {class: "unit"}, unit));
}
function banner(text, cls) { $("banners").appendChild(el("div", {class: "banner " + cls}, text)); }
function riskColor(r) { return r < 0.33 ? "var(--blue)" : r < 0.66 ? "var(--warn-fill)" : "var(--critical)"; }
function riskWord(r) { return r < 0.33 ? "low" : r < 0.66 ? "elevated" : "high"; }

async function fetchJson(path, label) {
  const ctl = new AbortController();
  const timer = setTimeout(() => ctl.abort(), FETCH_TIMEOUT_MS);
  try {
    const resp = await fetch(path, {cache: "no-store", signal: ctl.signal});
    const body = await resp.json().catch(() => ({}));
    if (!resp.ok) throw new Error("HTTP " + resp.status + (body.reason ? " (" + body.reason + ")" : ""));
    return body;
  } catch (e) {
    throw new Error(label + ": " + (e.name === "AbortError" ? "no answer within 15 s" : e.message));
  } finally {
    clearTimeout(timer);
  }
}
const fetchSite = site => fetchJson("/site/" + site, site);

function renderHealth(h) {
  const pill = $("health");
  const ok = h && h.status === "ok";
  pill.className = "pill " + (h ? (ok ? "ok" : "degraded") : "unknown");
  const icon = ok ? "M20 6L9 17l-5-5" : "M12 8v5m0 3h.01M10.3 3.9L2.4 18a2 2 0 001.7 3h15.8a2 2 0 001.7-3L13.7 3.9a2 2 0 00-3.4 0z";
  pill.querySelector("svg").replaceChildren(svgEl("path", {d: icon, fill: "none", stroke: "currentColor", "stroke-width": "2", "stroke-linecap": "round", "stroke-linejoin": "round"}));
  $("health-text").textContent = h ? (ok ? "Ingestion running" : "Ingestion degraded") : "Ingestion status unknown";
  if (h) pill.title = "Luchtmeetnet last fetch " + local(h.luchtmeetnet.last_successful_fetch) + ", bad data last hour " + h.luchtmeetnet.bad_data_count
    + ". NDW last fetch " + local(h.ndw.last_successful_fetch) + ", bad data last hour " + h.ndw.bad_data_count + ".";
}

function renderKpis(data) {
  const ok = Object.values(data);
  const air = ok.find(d => has(d.no2_ug_m3));
  setValue("no2", air && air.no2_ug_m3, 1, "\u00b5g/m\u00b3");
  const meter = $("no2-meter"), threshold = __THRESHOLD__;
  const scaleMax = Math.max(2 * threshold, air ? air.no2_ug_m3 * 1.15 : 0);
  meter.setAttribute("aria-valuemax", Math.round(scaleMax));
  meter.setAttribute("aria-valuenow", air ? air.no2_ug_m3 : 0);
  meter.firstElementChild.style.width = air ? Math.min(100, air.no2_ug_m3 / scaleMax * 100) + "%" : "0";
  $("no2-tick").style.left = threshold / scaleMax * 100 + "%";
  $("no2-tick-label").style.left = threshold / scaleMax * 100 + "%";
  $("no2-time").textContent = air ? "Hour ending " + local(air.timestamp) + ", Luchtmeetnet NL10240" : "No NO2 reading available";
  if (!air) banner("No NO2 reading available.", "warn");
  else {
    if (ageMin(air.timestamp) > AIR_MAX_MIN) banner("Stale NO2: the newest reading (hour ending " + local(air.timestamp) + ") is older than 2 h.", "warn");
    if (air.no2_is_flagged) banner("The newest NO2 reading (hour ending " + local(air.timestamp) + ") is flagged by ingestion as stale or suspect (a value repeated hour after hour) and is left out of training. Treat it with caution.", "warn");
  }

  const pred = ok.find(d => has(d.no2_ug_m3_predicted));
  setValue("pred", pred && pred.no2_ug_m3_predicted, 1, "\u00b5g/m\u00b3");
  const rm = $("risk-meter"), r = pred ? pred.no2_exceedance_risk : null;
  rm.setAttribute("aria-valuenow", has(r) ? r : 0);
  rm.firstElementChild.style.width = has(r) ? r * 100 + "%" : "0";
  rm.firstElementChild.style.background = has(r) ? riskColor(r) : "transparent";
  $("risk").textContent = has(r) ? "Exceedance risk " + num(r, 2) + " (" + riskWord(r) + "), hour " + windowText(pred.traffic_window_end) : "No prediction for this hour";
  [...new Set(ok.map(d => d.prediction_error).filter(has))].forEach(e => banner("Prediction unavailable: " + e, "err"));

  const tot = ok.find(d => has(d.total_intensity_veh_per_hr));
  setValue("total", tot && tot.total_intensity_veh_per_hr, 0, "veh/h");
  const shares = $("shares");
  shares.replaceChildren();
  if (tot) SITES.forEach(s => {
    const v = data[s] && data[s].intensity_veh_per_hr;
    if (has(v)) shares.appendChild(el("span", {style: "flex:" + v + " 1 0; background:" + SWATCH[s]}));
  });
  $("traffic-time").textContent = tot ? "Hourly mean, " + windowText(tot.traffic_window_end) + " (hrl, hrr, vwd, vwa shares above)"
    : "No current hourly window shared by all four sites";
  const ends = ok.map(d => d.traffic_window_end).filter(has).sort();
  if (!ends.length) banner("No traffic data available.", "warn");
  else if (new Date(ends[0]) < hourStart()) banner("Stale traffic: the oldest site window (" + windowText(ends[0]) + ") ended before the current hour.", "warn");

  const tbody = $("rows");
  tbody.replaceChildren();
  const maxI = Math.max(1, ...ok.map(d => d.intensity_veh_per_hr || 0));
  SITES.forEach(s => {
    const d = data[s] || {};
    const tr = el("tr");
    const site = el("div", {class: "site"});
    site.appendChild(el("i", {style: "background:" + SWATCH[s]}));
    const name = el("div", {}, s);
    name.appendChild(el("small", {}, NAMES[s]));
    site.appendChild(name);
    tr.appendChild(el("td")).appendChild(site);
    const tdI = el("td", {class: "num"}, num(d.intensity_veh_per_hr, 0));
    if (has(d.intensity_veh_per_hr)) { const bar = el("span", {class: "bar"}); bar.appendChild(el("span", {style: "width:" + (d.intensity_veh_per_hr / maxI * 100) + "%"})); tdI.appendChild(bar); }
    tr.appendChild(tdI);
    tr.appendChild(el("td", {}, has(d.traffic_window_end) ? windowText(d.traffic_window_end) + (has(d.traffic_n_samples) ? ", " + d.traffic_n_samples + " samples" : "") : "n/a"));
    tr.appendChild(el("td", {class: "num"}, num(d.no2_ug_m3_predicted, 1)));
    const tdR = el("td");
    if (has(d.no2_exceedance_risk)) {
      const wrap = el("span", {class: "risk"});
      const m = el("span", {class: "m"}); m.appendChild(el("span", {style: "width:" + (d.no2_exceedance_risk * 100) + "%; background:" + riskColor(d.no2_exceedance_risk)}));
      wrap.appendChild(m); wrap.appendChild(document.createTextNode(num(d.no2_exceedance_risk, 2) + " ")); wrap.appendChild(el("small", {}, riskWord(d.no2_exceedance_risk)));
      tdR.appendChild(wrap);
    } else tdR.textContent = "n/a";
    tr.appendChild(tdR);
    tbody.appendChild(tr);
  });

  const m = ok.find(d => has(d.model_trained_at));
  $("model").textContent = m ? "Model: linear regression on total traffic and hour of day, trained " + local(m.model_trained_at) + " on " + m.model_n_rows
    + " hour" + (m.model_n_rows === 1 ? "" : "s") + " of this pipeline's own data" + (m.model_n_rows < 10 ? ", too few to be reliable yet." : ".") : "Model: no metadata.";
}

// --- 24-hour chart: NO2 line above, traffic columns below, one shared crosshair ---------------
const chart = {hours: [], no2: new Map(), traffic: new Map(), idx: -1};
const W = 1000, PAD = {l: 56, r: 24}, NO2_Y = {top: 28, h: 190}, TR_Y = {top: 262, h: 120};

function niceMax(v) { if (!(v > 0)) return 1; const p = Math.pow(10, Math.floor(Math.log10(v))); const n = v / p; return (n <= 1 ? 1 : n <= 2 ? 2 : n <= 5 ? 5 : 10) * p; }
function ticks(max, count) { const step = niceMax(max / count); const out = []; for (let v = 0; v <= max + 1e-9; v += step) out.push(v); return out; }

function renderHistory(hist) {
  const svg = $("svg");
  svg.replaceChildren();
  const end = hourStart().getTime() + HOUR_MS;          // the hour filling now, labelled by its end
  const start = end - 24 * HOUR_MS;
  chart.hours = []; chart.no2 = new Map(); chart.traffic = new Map();
  for (let t = start; t <= end; t += HOUR_MS) chart.hours.push(t);
  (hist.no2 || []).forEach(p => chart.no2.set(new Date(p.timestamp).getTime(), p));
  (hist.traffic || []).forEach(p => chart.traffic.set(new Date(p.window_end).getTime(), p));
  const innerW = W - PAD.l - PAD.r, hourW = innerW / 24;
  const x = t => PAD.l + (t - start) / HOUR_MS * hourW - hourW / 2;      // the hour's midpoint
  const no2Max = niceMax(Math.max(hist.threshold_ug_m3 || 40, ...[...chart.no2.values()].map(p => p.value || 0),
    ...[...chart.traffic.values()].map(p => p.no2_ug_m3_predicted || 0)) * 1.15);
  const trMax = niceMax(Math.max(1000, ...[...chart.traffic.values()].map(p => p.total_intensity_veh_per_hr || 0)) * 1.1);
  const yN = v => NO2_Y.top + NO2_Y.h - v / no2Max * NO2_Y.h;
  const yT = v => TR_Y.top + TR_Y.h - v / trMax * TR_Y.h;

  svg.appendChild(svgEl("text", {x: PAD.l, y: 14, class: "ttl"}, "NO2 (\u00b5g/m\u00b3)"));
  svg.appendChild(svgEl("text", {x: PAD.l, y: TR_Y.top - 12, class: "ttl"}, "Total traffic (veh/h)"));
  ticks(no2Max, 4).forEach(v => {
    svg.appendChild(svgEl("line", {x1: PAD.l, x2: W - PAD.r, y1: yN(v), y2: yN(v), class: v === 0 ? "base" : "grid"}));
    svg.appendChild(svgEl("text", {x: PAD.l - 8, y: yN(v) + 4, "text-anchor": "end"}, num(v, 0)));
  });
  ticks(trMax, 3).forEach(v => {
    svg.appendChild(svgEl("line", {x1: PAD.l, x2: W - PAD.r, y1: yT(v), y2: yT(v), class: v === 0 ? "base" : "grid"}));
    svg.appendChild(svgEl("text", {x: PAD.l - 8, y: yT(v) + 4, "text-anchor": "end"}, num(v, 0)));
  });
  const thr = hist.threshold_ug_m3 || 40;
  if (thr < no2Max) {
    svg.appendChild(svgEl("line", {x1: PAD.l, x2: W - PAD.r, y1: yN(thr), y2: yN(thr), class: "ref"}));
    svg.appendChild(svgEl("text", {x: W - PAD.r, y: yN(thr) - 5, "text-anchor": "end", class: "lbl"}, num(thr, 0) + " EU annual limit"));
  }
  chart.hours.forEach((t, i) => {
    if (i % 3 === 0) svg.appendChild(svgEl("text", {x: x(t), y: TR_Y.top + TR_Y.h + 18, "text-anchor": "middle"}, windowText(new Date(t).toISOString()).split("-")[0]));
  });

  const pts = chart.hours.filter(t => chart.no2.has(t) && has(chart.no2.get(t).value)).map(t => [x(t), yN(chart.no2.get(t).value), t]);
  if (pts.length > 1) {
    const d = pts.map((p, i) => (i ? "L" : "M") + p[0].toFixed(1) + " " + p[1].toFixed(1)).join(" ");
    svg.appendChild(svgEl("path", {d: d + " L" + pts[pts.length - 1][0].toFixed(1) + " " + yN(0) + " L" + pts[0][0].toFixed(1) + " " + yN(0) + " Z", class: "wash"}));
    svg.appendChild(svgEl("path", {d, class: "series"}));
  }
  pts.forEach(p => svg.appendChild(svgEl("circle", {cx: p[0], cy: p[1], r: 4, class: "dot" + (chart.no2.get(p[2]).is_flagged ? " flag" : "")})));
  if (pts.length) { const last = pts[pts.length - 1]; svg.appendChild(svgEl("text", {x: last[0] + 8, y: last[1] + 4, class: "lbl"}, num(chart.no2.get(last[2]).value, 1))); }
  // The model's prediction for every past hour that had traffic: how well it tracks reality.
  const ppts = chart.hours.filter(t => chart.traffic.has(t) && has(chart.traffic.get(t).no2_ug_m3_predicted))
    .map(t => [x(t), yN(chart.traffic.get(t).no2_ug_m3_predicted), t]);
  if (ppts.length > 1) svg.appendChild(svgEl("path", {d: ppts.map((p, i) => (i ? "L" : "M") + p[0].toFixed(1) + " " + p[1].toFixed(1)).join(" "), class: "series pred"}));
  ppts.forEach(p => svg.appendChild(svgEl("circle", {cx: p[0], cy: p[1], r: 4, class: "dot pred"})));
  if (ppts.length) {
    const last = ppts[ppts.length - 1], lastM = pts.length ? pts[pts.length - 1] : null;
    // keep the two end labels apart when the lines end close together
    const ly = lastM && Math.abs(lastM[1] - last[1]) < 18 && Math.abs(lastM[0] - last[0]) < 80 ? last[1] + 22 : last[1] + 4;
    svg.appendChild(svgEl("text", {x: last[0] + 8, y: ly, class: "lbl"}, num(chart.traffic.get(last[2]).no2_ug_m3_predicted, 1) + " predicted"));
  }
  const colW = Math.min(24, hourW - 4);
  chart.hours.forEach(t => {
    const p = chart.traffic.get(t);
    if (!p) return;
    const h = Math.max(0, yT(0) - yT(p.total_intensity_veh_per_hr));
    svg.appendChild(svgEl("path", {class: "col", "data-t": t, d: roundedColumn(x(t) - colW / 2, yT(0) - h, colW, h, 4)}));
  });
  svg.appendChild(svgEl("line", {x1: 0, x2: 0, y1: NO2_Y.top, y2: TR_Y.top + TR_Y.h, class: "hair", id: "hair"}));
  svg.appendChild(svgEl("rect", {x: PAD.l, y: NO2_Y.top, width: innerW, height: TR_Y.top + TR_Y.h - NO2_Y.top, class: "hit", id: "hit"}));
  chart.x = x; chart.innerW = innerW; chart.hourW = hourW;

  const rows = $("hist-rows");
  rows.replaceChildren();
  chart.hours.forEach(t => {
    const n = chart.no2.get(t), tr = chart.traffic.get(t);
    if (!n && !tr) return;
    const row = el("tr");
    row.appendChild(el("td", {}, windowText(new Date(t).toISOString())));
    row.appendChild(el("td", {class: "num"}, n ? num(n.value, 1) + (n.is_flagged ? " (flagged)" : "") : "n/a"));
    row.appendChild(el("td", {class: "num"}, tr && has(tr.no2_ug_m3_predicted) ? num(tr.no2_ug_m3_predicted, 1) : "n/a"));
    row.appendChild(el("td", {class: "num"}, tr ? num(tr.total_intensity_veh_per_hr, 0) : "n/a"));
    rows.appendChild(row);
  });
  if (!chart.no2.size && !chart.traffic.size) svg.appendChild(svgEl("text", {x: W / 2, y: 200, "text-anchor": "middle", class: "lbl"}, "No readings in the last 24 hours"));
  showIndex(-1);
}

function roundedColumn(x, y, w, h, r) {
  if (h <= r) return "M" + x + " " + (y + h) + " h" + w + " v" + (-h) + " h" + (-w) + " Z";
  return "M" + x + " " + (y + h) + " v" + (-(h - r)) + " a" + r + " " + r + " 0 0 1 " + r + " " + (-r) + " h" + (w - 2 * r)
    + " a" + r + " " + r + " 0 0 1 " + r + " " + r + " v" + (h - r) + " Z";
}

function showIndex(i) {
  chart.idx = i;
  const box = $("chart"), tip = $("tip"), hair = $("hair");
  document.querySelectorAll("#svg .col").forEach(c => c.classList.remove("hot"));
  if (i < 0 || i >= chart.hours.length) { box.classList.remove("active"); tip.classList.remove("show"); return; }
  const t = chart.hours[i], px = chart.x(t);
  hair.setAttribute("x1", px); hair.setAttribute("x2", px);
  box.classList.add("active");
  const col = document.querySelector('#svg .col[data-t="' + t + '"]');
  if (col) col.classList.add("hot");
  const n = chart.no2.get(t), tr = chart.traffic.get(t);
  tip.replaceChildren(el("div", {class: "t"}, windowText(new Date(t).toISOString()) + " local"));
  const r1 = el("div", {class: "row"}); r1.appendChild(el("span", {}, "")); r1.firstChild.appendChild(el("i", {style: "background:var(--blue)"})); r1.firstChild.appendChild(document.createTextNode("NO2"));
  r1.appendChild(el("b", {}, n ? num(n.value, 1) + " \u00b5g/m\u00b3" + (n.is_flagged ? " (flagged)" : "") : "no reading")); tip.appendChild(r1);
  const r2 = el("div", {class: "row"}); r2.appendChild(el("span", {}, "")); r2.firstChild.appendChild(el("i", {style: "background:var(--orange)"})); r2.firstChild.appendChild(document.createTextNode("Traffic"));
  r2.appendChild(el("b", {}, tr ? num(tr.total_intensity_veh_per_hr, 0) + " veh/h" : "no full hour")); tip.appendChild(r2);
  const r3 = el("div", {class: "row"}); r3.appendChild(el("span", {}, "")); r3.firstChild.appendChild(el("i", {class: "dash"})); r3.firstChild.appendChild(document.createTextNode("Predicted"));
  r3.appendChild(el("b", {}, tr && has(tr.no2_ug_m3_predicted) ? num(tr.no2_ug_m3_predicted, 1) + " \u00b5g/m\u00b3" : "no prediction")); tip.appendChild(r3);
  // The svg can be wider than its scrolling wrapper on a phone, so measure the svg itself.
  const svgRect = $("svg").getBoundingClientRect(), boxRect = box.getBoundingClientRect(), scale = svgRect.width / W;
  const left = Math.min(Math.max(8, svgRect.left - boxRect.left + px * scale + 14), boxRect.width - tip.offsetWidth - 8);
  tip.style.left = left + "px"; tip.style.top = (NO2_Y.top * scale) + "px";
  tip.classList.add("show");
}

function wireChart() {
  const box = $("chart");
  box.addEventListener("pointermove", e => {
    if (!chart.hours.length) return;
    const rect = $("svg").getBoundingClientRect(), vx = (e.clientX - rect.left) / rect.width * W;
    const i = Math.round((vx - PAD.l) / chart.hourW - 0.5);
    showIndex(Math.max(0, Math.min(chart.hours.length - 1, i)));
  });
  box.addEventListener("pointerleave", () => showIndex(-1));
  box.addEventListener("keydown", e => {
    if (!chart.hours.length) return;
    if (e.key === "ArrowLeft" || e.key === "ArrowRight") {
      e.preventDefault();
      const cur = chart.idx < 0 ? chart.hours.length - 1 : chart.idx;
      showIndex(Math.max(0, Math.min(chart.hours.length - 1, cur + (e.key === "ArrowRight" ? 1 : -1))));
    } else if (e.key === "Escape") showIndex(-1);
  });
  box.addEventListener("blur", () => showIndex(-1));
}

async function refresh() {
  const [sites, health, history] = await Promise.all([
    Promise.allSettled(SITES.map(fetchSite)),
    fetchJson("/health", "health").catch(() => null),
    fetchJson("/history?hours=24", "history").catch(e => ({error: e.message})),
  ]);
  const data = {};
  $("banners").replaceChildren();
  sites.forEach((r, i) => { if (r.status === "fulfilled") data[SITES[i]] = r.value; else banner("Could not load " + r.reason.message, "err"); });
  renderHealth(health);
  renderKpis(data);
  if (history && !history.error) renderHistory(history); else banner("Could not load the 24-hour history" + (history && history.error ? ": " + history.error : ""), "err");
  $("updated").textContent = "Updated " + clock(new Date().toISOString()) + " (Europe/Amsterdam). Refreshes every 60 s.";
}

let busy = false;
async function tick() {
  if (busy) return;  // the previous round is still waiting: do not pile up requests
  busy = true;
  try { await refresh(); } finally { busy = false; }
}

wireChart();
tick();
setInterval(tick, 60000);
</script>
</body>
</html>
""".replace("__THRESHOLD__", f"{THRESHOLD:g}").replace("__MIN_SAMPLES__", str(MIN_SAMPLES)) \
    .replace("__MIN_SPAN__", str(int(MIN_SPAN.total_seconds() // 60)))
