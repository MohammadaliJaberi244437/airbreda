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

GET /health reads ingestion_runs only: the ingestion containers run and exit, so that
table is the only record of whether they are working.
"""
import csv
import logging
import os
import threading
import time
from contextlib import asynccontextmanager, closing
from datetime import datetime, timedelta, timezone

import boto3
import psycopg2
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError, CredentialRetrievalError, \
    NoCredentialsError, PartialCredentialsError
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse

from common import S3Store, get_conn, log_event, setup_logging
from features import MIN_SAMPLES, MIN_SPAN, SITE_LABELS, hourly_site_windows, ndw_key_site, \
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


class SourceUnavailable(Exception):
    """A backing store is down or not configured; `reason` is safe to show to clients."""

    def __init__(self, source, reason, detail=None):
        super().__init__(f"{source}: {reason}")
        self.source, self.reason, self.detail = source, reason, detail


@asynccontextmanager
async def lifespan(_app):
    setup_logging()
    # uvicorn installs plain-text handlers: send its logs through the JSON root handler and
    # drop its access log, which log_requests below replaces.
    for name in ("uvicorn", "uvicorn.error"):
        logging.getLogger(name).handlers.clear()
        logging.getLogger(name).propagate = True
    logging.getLogger("uvicorn.access").disabled = True
    yield


app = FastAPI(title="AirBreda dashboard", lifespan=lifespan)


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
            error = f"prediction failed: {type(exc).__name__}: {exc}"
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


@app.get("/", response_class=HTMLResponse)
def index():
    return PAGE


PAGE = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>AirBreda</title>
<style>
:root { --bg:#f6f6f4; --card:#fff; --ink:#1d1d1f; --muted:#5f6268; --line:#e2e2df;
  --warn-bg:#fff3d1; --warn-ink:#5e4300; --err-bg:#fde6e6; --err-ink:#7f1717; --bar:#2a6fdb; }
@media (prefers-color-scheme: dark) { :root { --bg:#141518; --card:#1d1f23; --ink:#ececec;
  --muted:#a3a6ac; --line:#30333a; --warn-bg:#3a2f12; --warn-ink:#f3d58a; --err-bg:#3d1c1c;
  --err-ink:#f4b4b4; --bar:#6aa0ff; } }
body { margin:0; background:var(--bg); color:var(--ink);
  font:15px/1.45 system-ui, -apple-system, "Segoe UI", sans-serif; }
main { max-width:940px; margin:0 auto; padding:24px 16px 40px; }
h1 { font-size:22px; margin:0 0 4px; }
.meta { color:var(--muted); font-size:13px; }
.cards { display:grid; grid-template-columns:repeat(auto-fit, minmax(210px, 1fr)); gap:12px; margin:16px 0; }
.card { background:var(--card); border:1px solid var(--line); border-radius:10px; padding:14px 16px; }
.label { color:var(--muted); font-size:12px; text-transform:uppercase; letter-spacing:.04em; }
.value { font-size:28px; font-weight:600; font-variant-numeric:tabular-nums; }
.unit { font-size:14px; color:var(--muted); font-weight:400; }
.banner { border-radius:8px; padding:10px 14px; margin:8px 0; font-weight:500; }
.warn { background:var(--warn-bg); color:var(--warn-ink); }
.err { background:var(--err-bg); color:var(--err-ink); }
.scroll { overflow-x:auto; }
table { width:100%; border-collapse:collapse; background:var(--card); border:1px solid var(--line); }
th, td { text-align:left; padding:9px 12px; border-bottom:1px solid var(--line); font-variant-numeric:tabular-nums; white-space:nowrap; }
th { font-size:12px; color:var(--muted); font-weight:600; }
.num { text-align:right; }
.bar { display:inline-block; width:56px; height:6px; background:var(--line); border-radius:3px; margin-left:8px; vertical-align:middle; }
.bar span { display:block; height:100%; background:var(--bar); border-radius:3px; }
</style>
</head>
<body>
<main>
<h1>AirBreda: NO<sub>2</sub> and A27 traffic</h1>
<div class="meta">Luchtmeetnet station NL10240 (Breda) and four NDW measuring sites on the A27. Refreshes every 60 s.</div>
<div id="banners"></div>
<div class="cards">
  <div class="card"><div class="label">Measured NO<sub>2</sub></div><div class="value" id="no2">-</div><div class="meta" id="no2-time"></div></div>
  <div class="card"><div class="label">Predicted NO<sub>2</sub></div><div class="value" id="pred">-</div><div class="meta" id="risk"></div></div>
  <div class="card"><div class="label">Total traffic, 4 sites</div><div class="value" id="total">-</div><div class="meta" id="traffic-time"></div></div>
</div>
<div class="scroll"><table>
<thead><tr><th>Site</th><th class="num">Traffic (veh/h, hourly mean)</th><th>Hourly window (local)</th>
<th class="num">Predicted NO<sub>2</sub> (&micro;g/m&sup3;)</th><th class="num">Exceedance risk</th></tr></thead>
<tbody id="rows"></tbody>
</table></div>
<p class="meta" id="updated"></p>
<p class="meta">Traffic is the mean of a site's samples in its newest clock hour with at least __MIN_SAMPLES__ samples
spanning __MIN_SPAN__ minutes, the rule the training data uses; until the current hour has that many, the hour that
just ended is shown. The prediction uses the total of all four sites in one shared hourly window and the local hour
of that window, so it is the same for every site. Exceedance risk is a 0-1 score, not a calibrated probability: a
sigmoid of predicted NO<sub>2</sub> minus __THRESHOLD__ &micro;g/m&sup3; (0.5 at __THRESHOLD__; set by NO2_THRESHOLD,
default 40, the EU annual limit, used here as an hourly reference).</p>
<p class="meta" id="model"></p>
</main>
<script>
const SITES = ["hrl", "hrr", "vwd", "vwa"];
const NAMES = {hrl: "hrl (A27 main, dir. 1)", hrr: "hrr (A27 main, dir. 2)",
               vwd: "vwd (entry slip road)", vwa: "vwa (exit slip road)"};
const AIR_MAX_MIN = 120, FETCH_TIMEOUT_MS = 15000, HOUR_MS = 3600000;
const $ = id => document.getElementById(id);
const has = v => v !== null && v !== undefined;
const show = v => has(v) ? String(v) : "n/a";
const ageMin = iso => (Date.now() - new Date(iso).getTime()) / 60000;
const TZ = {timeZone: "Europe/Amsterdam"};
const local = iso => has(iso) ? new Date(iso).toLocaleString("en-GB", {...TZ,
  day: "2-digit", month: "short", hour: "2-digit", minute: "2-digit"}) : "n/a";
const clock = iso => new Date(iso).toLocaleTimeString("en-GB", {...TZ, hour: "2-digit", minute: "2-digit"});
// A window is labelled by its END, like Luchtmeetnet's hours: show it as start-end.
const windowText = end => has(end)
  ? local(new Date(new Date(end).getTime() - HOUR_MS).toISOString()) + "-" + clock(end) : "n/a";
const hourStart = () => { const d = new Date(); d.setUTCMinutes(0, 0, 0); return d; };

function cell(row, text, cls) {
  const td = document.createElement("td");
  td.textContent = text;
  if (cls) td.className = cls;
  row.appendChild(td);
  return td;
}

function banner(text, cls) {
  const div = document.createElement("div");
  div.className = "banner " + cls;
  div.textContent = text;
  $("banners").appendChild(div);
}

async function fetchSite(site) {
  const ctl = new AbortController();
  const timer = setTimeout(() => ctl.abort(), FETCH_TIMEOUT_MS);
  try {
    const resp = await fetch("/site/" + site, {cache: "no-store", signal: ctl.signal});
    const body = await resp.json().catch(() => ({}));
    if (!resp.ok) throw new Error("HTTP " + resp.status + (body.reason ? " (" + body.reason + ")" : ""));
    return body;
  } catch (e) {
    throw new Error(site + ": " + (e.name === "AbortError" ? "no answer within 15 s" : e.message));
  } finally {
    clearTimeout(timer);
  }
}

async function refresh() {
  const results = await Promise.allSettled(SITES.map(fetchSite));
  const data = {};
  $("banners").replaceChildren();
  results.forEach((r, i) => {
    if (r.status === "fulfilled") data[SITES[i]] = r.value;
    else banner("Could not load " + r.reason.message, "err");
  });
  const ok = Object.values(data);

  const air = ok.find(d => has(d.no2_ug_m3));
  $("no2").replaceChildren(show(air && air.no2_ug_m3));
  $("no2").insertAdjacentHTML("beforeend", ' <span class="unit">&micro;g/m&sup3;</span>');
  $("no2-time").textContent = air ? "hour ending " + local(air.timestamp) : "no reading";
  if (!air) banner("No NO2 reading available.", "warn");
  else {
    if (ageMin(air.timestamp) > AIR_MAX_MIN)
      banner("Stale NO2: the newest reading (hour ending " + local(air.timestamp) + ") is older than 2 h.", "warn");
    if (air.no2_is_flagged)
      banner("The newest NO2 reading (hour ending " + local(air.timestamp) + ") is flagged by ingestion as stale "
        + "or suspect (a value repeated hour after hour) and is left out of training. Treat it with caution.", "warn");
  }

  const pred = ok.find(d => has(d.no2_ug_m3_predicted));
  $("pred").replaceChildren(show(pred && pred.no2_ug_m3_predicted));
  $("pred").insertAdjacentHTML("beforeend", ' <span class="unit">&micro;g/m&sup3;</span>');
  $("risk").textContent = pred ? "exceedance risk " + show(pred.no2_exceedance_risk) : "no prediction";
  [...new Set(ok.map(d => d.prediction_error).filter(has))].forEach(e => banner("Prediction unavailable: " + e, "err"));

  const tot = ok.find(d => has(d.total_intensity_veh_per_hr));
  $("total").replaceChildren(show(tot && tot.total_intensity_veh_per_hr));
  $("total").insertAdjacentHTML("beforeend", ' <span class="unit">veh/h</span>');
  $("traffic-time").textContent = tot ? "hourly window " + windowText(tot.traffic_window_end)
    : "no current hourly window shared by all four sites";
  const ends = ok.map(d => d.traffic_window_end).filter(has).sort();
  if (!ends.length) banner("No traffic data available.", "warn");
  else if (new Date(ends[0]) < hourStart())
    banner("Stale traffic: the oldest site window (" + windowText(ends[0]) + ") ended before the current hour.", "warn");

  const tbody = $("rows");
  tbody.replaceChildren();
  SITES.forEach(s => {
    const d = data[s] || {};
    const tr = document.createElement("tr");
    cell(tr, NAMES[s]);
    cell(tr, show(d.intensity_veh_per_hr), "num");
    cell(tr, windowText(d.traffic_window_end) + (has(d.traffic_n_samples) ? ", " + d.traffic_n_samples + " samples" : ""));
    cell(tr, show(d.no2_ug_m3_predicted), "num");
    const td = cell(tr, show(d.no2_exceedance_risk), "num");
    if (has(d.no2_exceedance_risk)) {
      td.insertAdjacentHTML("beforeend", '<span class="bar"><span></span></span>');
      td.querySelector(".bar span").style.width = (d.no2_exceedance_risk * 100) + "%";
    }
    tbody.appendChild(tr);
  });

  const newest = ok.map(d => d.traffic_timestamp).filter(has).sort().pop();
  $("updated").textContent = "Last updated " + local(new Date().toISOString()) + " (Europe/Amsterdam). NO2: hour ending "
    + local(air && air.timestamp) + ". Traffic: newest sample used " + local(newest) + ".";
  const m = ok.find(d => has(d.model_trained_at));
  $("model").textContent = m ? "Model: linear regression trained " + local(m.model_trained_at) + " on "
    + m.model_n_rows + " hour(s)" + (m.model_n_rows < 10 ? ", too few to be reliable." : ".") : "Model: no metadata.";
}

let busy = false;
async function tick() {
  if (busy) return;  // the previous round is still waiting: do not pile up requests
  busy = true;
  try { await refresh(); } finally { busy = false; }
}

tick();
setInterval(tick, 60000);
</script>
</body>
</html>
""".replace("__THRESHOLD__", f"{THRESHOLD:g}").replace("__MIN_SAMPLES__", str(MIN_SAMPLES)) \
    .replace("__MIN_SPAN__", str(int(MIN_SPAN.total_seconds() // 60)))
