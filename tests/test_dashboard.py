"""Dashboard endpoints with every data source replaced: no DB, S3 or network."""
import logging
from datetime import datetime, timedelta, timezone

import psycopg2
import pytest
from botocore.exceptions import ClientError, EndpointConnectionError
from fastapi.testclient import TestClient

import dashboard
from features import SITE_LABELS, window_hour_of_day

UTC = timezone.utc
REAL_LATEST_NO2, REAL_TRAFFIC_SNAPSHOT = dashboard.latest_no2, dashboard.traffic_snapshot
NO2 = (datetime(2026, 10, 1, 9, tzinfo=UTC), 32.68, False)
# The hour that just ended: a current window, whatever time the tests run.
WINDOW_END = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
TRAFFIC = {site: {"intensity": value, "measured_at": WINDOW_END - timedelta(minutes=4),
                  "window_end": WINDOW_END, "n_samples": 6}
           for site, value in zip(SITE_LABELS, (1350.0, 900.0, 300.0, 240.0))}
RESPONSE_KEYS = {"site_id", "no2_ug_m3", "intensity_veh_per_hr", "no2_exceedance_risk",
                 "timestamp", "no2_is_flagged", "no2_ug_m3_predicted", "no2_threshold_ug_m3",
                 "total_intensity_veh_per_hr", "traffic_window_end", "traffic_n_samples",
                 "traffic_timestamp", "model_trained_at", "model_n_rows", "prediction_error"}


@pytest.fixture
def predict_calls(monkeypatch):
    calls = []
    monkeypatch.setattr(dashboard, "predict", lambda total, hour: calls.append((total, hour)) or {
        "no2_ug_m3_predicted": 30.0, "no2_exceedance_risk": 0.1192})
    return calls


@pytest.fixture
def client(monkeypatch, predict_calls):
    monkeypatch.setattr(dashboard, "latest_no2", lambda: NO2)
    monkeypatch.setattr(dashboard, "traffic_snapshot", lambda: TRAFFIC)
    monkeypatch.setattr(dashboard, "model_meta",
                        lambda: {"trained_at": "2026-10-01T09:30:00+00:00", "n_rows": 1})
    return TestClient(dashboard.app)


def _events(caplog, name, level=logging.ERROR):
    return [r for r in caplog.records
            if f'"event": "{name}"' in r.getMessage() and r.levelno == level]


# --- /site ---------------------------------------------------------------------------

def test_site_returns_real_readings_and_the_prediction(client, predict_calls):
    resp = client.get("/site/hrl")
    assert resp.status_code == 200
    body = resp.json()
    assert set(body) == RESPONSE_KEYS
    assert body["site_id"] == "hrl" and body["no2_ug_m3"] == 32.68
    assert body["timestamp"] == "2026-10-01T09:00:00+00:00" and body["no2_is_flagged"] is False
    assert body["intensity_veh_per_hr"] == 1350.0 and body["traffic_n_samples"] == 6
    assert body["traffic_window_end"] == WINDOW_END.isoformat()
    assert body["total_intensity_veh_per_hr"] == 2790.0
    assert body["no2_ug_m3_predicted"] == 30.0 and body["no2_exceedance_risk"] == 0.1192
    assert body["no2_threshold_ug_m3"] == dashboard.THRESHOLD
    assert body["prediction_error"] is None and body["model_n_rows"] == 1
    # The hour fed to the model is the traffic window's own hour, as in training.
    assert predict_calls == [(2790.0, window_hour_of_day(WINDOW_END))]


def test_flagged_no2_is_returned_with_its_flag(client, monkeypatch):
    monkeypatch.setattr(dashboard, "latest_no2", lambda: (NO2[0], 41.0, True))
    body = client.get("/site/hrl").json()
    assert body["no2_ug_m3"] == 41.0 and body["no2_is_flagged"] is True


def test_failing_prediction_still_returns_200_with_the_real_values(client, monkeypatch, caplog):
    def broken(total, hour):
        raise RuntimeError("model.pkl is corrupt")
    monkeypatch.setattr(dashboard, "predict", broken)
    with caplog.at_level(logging.INFO):
        resp = client.get("/site/vwd")
    assert resp.status_code == 200
    body = resp.json()
    assert body["no2_exceedance_risk"] is None and body["no2_ug_m3_predicted"] is None
    assert "model.pkl is corrupt" in body["prediction_error"]
    assert body["no2_ug_m3"] == 32.68 and body["intensity_veh_per_hr"] == 300.0
    assert len(_events(caplog, "prediction_failed")) == 1
    assert len(_events(caplog, "request", logging.INFO)) == 1


def test_missing_site_traffic_skips_the_prediction(client, monkeypatch, predict_calls):
    monkeypatch.setattr(dashboard, "traffic_snapshot",
                        lambda: {s: t for s, t in TRAFFIC.items() if s != "vwa"})
    body = client.get("/site/hrl").json()
    assert body["total_intensity_veh_per_hr"] is None and body["no2_ug_m3_predicted"] is None
    assert body["intensity_veh_per_hr"] == 1350.0
    assert "vwa" in body["prediction_error"] and predict_calls == []


def test_misaligned_windows_skip_the_prediction(client, monkeypatch, predict_calls):
    old = WINDOW_END - timedelta(days=1)
    monkeypatch.setattr(dashboard, "traffic_snapshot", lambda: {
        **TRAFFIC, "vwa": {**TRAFFIC["vwa"], "intensity": 9999.0, "window_end": old}})
    body = client.get("/site/vwa").json()
    assert body["intensity_veh_per_hr"] == 9999.0
    assert body["total_intensity_veh_per_hr"] is None and body["no2_ug_m3_predicted"] is None
    assert body["prediction_error"].startswith("traffic windows not aligned")
    assert predict_calls == []


@pytest.mark.parametrize("end_offset_h, current", [(-1, False), (0, True), (1, True)])
def test_model_input_only_uses_the_current_or_just_ended_hour(end_offset_h, current):
    now = datetime(2026, 10, 1, 10, 39, tzinfo=UTC)
    end = datetime(2026, 10, 1, 10, tzinfo=UTC) + timedelta(hours=end_offset_h)
    traffic = {s: {**t, "window_end": end} for s, t in TRAFFIC.items()}
    total, label, error = dashboard.model_input(traffic, now)
    assert (total, label) == ((2790.0, end) if current else (None, None))
    assert (error is None) == current
    if not current:
        assert error.startswith("traffic stale")


def test_unknown_site_is_a_json_404(client):
    resp = client.get("/site/abc")
    assert resp.status_code == 404
    assert resp.json()["known_sites"] == list(SITE_LABELS)


def test_database_down_is_a_503_with_a_reason(client, monkeypatch, caplog):
    def refuse():
        raise psycopg2.OperationalError("connection to secret-host.rds refused")
    monkeypatch.setattr(dashboard, "latest_no2", REAL_LATEST_NO2)
    monkeypatch.setattr(dashboard, "get_conn", refuse)
    with caplog.at_level(logging.ERROR):
        resp = client.get("/site/hrl")
    assert resp.status_code == 503
    assert resp.json() == {"error": "service unavailable", "source": "database",
                           "reason": "database unreachable"}
    [logged] = _events(caplog, "source_unavailable")
    assert "secret-host" in logged.getMessage()  # detail is logged, not sent to the client


# --- S3 traffic snapshot ---------------------------------------------------------------

def _csv(site, samples, lanes=None):
    lines = ["measured_at,site,site_id,intensity_veh_per_hr,speed_kmh,lane_flows,lane_speeds,"
             "speed_invalid_count,captured_at"]
    lines += [f"{t},{site},X,{v},90.0,{lanes or v},90.0,0,{t}" for t, v in samples]
    return ("\r\n".join(lines) + "\r\n").encode()


def _hour(hour, value, minutes=(6, 16, 26, 36, 46, 56), day="2026-10-01"):
    return [(f"{day}T{hour:02d}:{m:02d}:00Z", value) for m in minutes]


class FakeStore:
    bucket = "test-bucket"

    def __init__(self, files, fail=None):
        self.files, self.fail, self.listed, self.read = files, fail, [], []
        self.client = self

    def list_objects_v2(self, Bucket, Prefix):
        if self.fail:
            raise self.fail
        self.listed.append(Prefix)
        return {"Contents": [{"Key": k} for k in self.files if k.startswith(Prefix)]}

    def get(self, key):
        self.read.append(key)
        return self.files.get(key)


def test_a_filling_hour_falls_back_to_the_hour_that_just_ended():
    files = {f"ndw/2026-10-01/08-{s}.csv": _csv(s, _hour(8, 1000)) for s in SITE_LABELS}
    # 09:12: the new hour holds one 1-minute sample per site, which must not be served.
    files.update({f"ndw/2026-10-01/09-{s}.csv": _csv(s, [("2026-10-01T09:06:00Z", 2280)])
                  for s in SITE_LABELS})
    traffic = dashboard.load_traffic(FakeStore(files), datetime(2026, 10, 1, 9, 12, tzinfo=UTC))
    assert traffic["hrl"] == {"intensity": 1000.0, "n_samples": 6,
                              "measured_at": datetime(2026, 10, 1, 8, 56, tzinfo=UTC),
                              "window_end": datetime(2026, 10, 1, 9, tzinfo=UTC)}


def test_the_filling_hour_is_used_once_it_covers_half_the_hour():
    files = {"ndw/2026-10-01/08-hrl.csv": _csv("hrl", _hour(8, 1000)),
             "ndw/2026-10-01/09-hrl.csv": _csv("hrl", _hour(9, 1500, minutes=(6, 16, 26, 36)))}
    traffic = dashboard.load_traffic(FakeStore(files), datetime(2026, 10, 1, 9, 41, tzinfo=UTC))
    assert traffic["hrl"]["intensity"] == 1500.0 and traffic["hrl"]["n_samples"] == 4
    assert traffic["hrl"]["window_end"] == datetime(2026, 10, 1, 10, tzinfo=UTC)


def test_yesterday_is_listed_only_when_today_has_too_few_files():
    files = {f"ndw/2026-09-30/23-{s}.csv": _csv(s, _hour(23, 700, day="2026-09-30"))
             for s in SITE_LABELS}
    store = FakeStore(files)
    traffic = dashboard.load_traffic(store, datetime(2026, 10, 1, 0, 5, tzinfo=UTC))
    assert store.listed == ["ndw/2026-10-01/", "ndw/2026-09-30/"]
    assert traffic["vwa"]["window_end"] == datetime(2026, 10, 1, 0, tzinfo=UTC)

    files = {f"ndw/2026-10-01/{h:02d}-{s}.csv": _csv(s, _hour(h, 700))
             for h in (3, 4, 5) for s in SITE_LABELS}
    store = FakeStore(files)
    dashboard.load_traffic(store, datetime(2026, 10, 1, 5, 5, tzinfo=UTC))
    assert store.listed == ["ndw/2026-10-01/"]
    assert len(store.read) == 4  # the newest file per site is enough


def test_samples_with_a_lane_in_data_error_are_not_served():
    files = {"ndw/2026-10-01/08-hrl.csv": _csv("hrl", _hour(8, 1140), lanes="None;1140.0")}
    assert dashboard.load_traffic(FakeStore(files), datetime(2026, 10, 1, 9, 5, tzinfo=UTC)) == {}


def test_an_unreadable_file_is_skipped(caplog):
    files = {"ndw/2026-10-01/08-hrl.csv": _csv("hrl", _hour(8, 1000)),
             "ndw/2026-10-01/09-hrl.csv": b"\xff\xfe\x00garbage"}
    with caplog.at_level(logging.WARNING):
        traffic = dashboard.load_traffic(FakeStore(files), datetime(2026, 10, 1, 9, 5, tzinfo=UTC))
    assert traffic["hrl"]["intensity"] == 1000.0
    assert len(_events(caplog, "traffic_file_unreadable", logging.WARNING)) == 1


@pytest.fixture
def real_snapshot(monkeypatch):
    monkeypatch.setattr(dashboard, "_traffic_cache", {"at": None, "value": None, "error": None})
    monkeypatch.setattr(dashboard, "traffic_snapshot", REAL_TRAFFIC_SNAPSHOT)


def test_traffic_snapshot_is_cached_for_60_seconds(monkeypatch, real_snapshot):
    calls = []
    monkeypatch.setattr(dashboard, "_store", FakeStore({}))
    monkeypatch.setattr(dashboard, "load_traffic", lambda store, now: calls.append(now) or TRAFFIC)
    assert dashboard.traffic_snapshot() is dashboard.traffic_snapshot() is TRAFFIC
    assert len(calls) == 1


def test_s3_down_is_a_503_and_the_failure_is_cached(client, monkeypatch, real_snapshot):
    store = FakeStore({}, fail=EndpointConnectionError(endpoint_url="https://s3.example"))
    monkeypatch.setattr(dashboard, "_store", store)
    for _ in range(3):
        resp = client.get("/site/hrl")
        assert resp.status_code == 503
        assert resp.json() == {"error": "service unavailable", "source": "s3",
                               "reason": "S3 unreachable"}
    store.fail = None
    assert client.get("/site/hrl").status_code == 503  # still inside S3_CACHE_SECONDS


@pytest.mark.parametrize("error, reason", [
    (RuntimeError("Credentials were refreshed, but the refreshed credentials are still expired."),
     "S3 credentials expired"),
    (ClientError({"Error": {"Code": "ExpiredToken", "Message": "token expired"}}, "ListObjectsV2"),
     "S3 credentials expired"),
    (ClientError({"Error": {"Code": "AccessDenied", "Message": "denied"}}, "ListObjectsV2"),
     "S3 access denied"),
    (ValueError("something else"), "S3 request failed"),
])
def test_any_s3_round_failure_is_a_503_with_a_reason(client, monkeypatch, real_snapshot,
                                                     error, reason):
    def fail(store, now):
        raise error
    monkeypatch.setattr(dashboard, "_store", FakeStore({}))
    monkeypatch.setattr(dashboard, "load_traffic", fail)
    resp = client.get("/site/hrl")
    assert resp.status_code == 503
    assert resp.json() == {"error": "service unavailable", "source": "s3", "reason": reason}


def test_dashboard_s3_client_fails_fast():
    assert dashboard.S3_CONFIG.connect_timeout <= 5 and dashboard.S3_CONFIG.read_timeout <= 10
    assert dashboard.S3_CONFIG.retries["total_max_attempts"] <= 2  # max_attempts counts retries


# --- /health ---------------------------------------------------------------------------

def test_health_shape_and_counts(client, monkeypatch):
    now = datetime.now(UTC)
    ndw_last = now - timedelta(minutes=5)
    monkeypatch.setattr(dashboard, "ingestion_summary", lambda: {
        "Luchtmeetnet": (now - timedelta(minutes=70), 0), "NDW": (ndw_last, 3)})
    body = client.get("/health").json()
    assert set(body) == {"status", "luchtmeetnet", "ndw", "checked_at"}
    assert body["status"] == "ok"
    assert body["ndw"] == {"last_successful_fetch": ndw_last.isoformat(), "bad_data_count": 3}
    assert body["luchtmeetnet"]["bad_data_count"] == 0


def test_health_source_without_any_success_is_null_and_degraded(client, monkeypatch):
    monkeypatch.setattr(dashboard, "ingestion_summary",
                        lambda: {"NDW": (datetime.now(UTC), 0)})
    body = client.get("/health").json()
    assert body["status"] == "degraded"
    assert body["luchtmeetnet"] == {"last_successful_fetch": None, "bad_data_count": 0}


@pytest.mark.parametrize("air_min, ndw_min, expected", [
    (70, 5, "ok"), (120, 30, "ok"), (121, 5, "degraded"), (70, 31, "degraded"),
    (None, 5, "degraded"), (70, None, "degraded"),
])
def test_health_degraded_rules(air_min, ndw_min, expected):
    now = datetime(2026, 10, 1, 12, tzinfo=UTC)
    ago = lambda m: None if m is None else now - timedelta(minutes=m)  # noqa: E731
    assert dashboard.health_status(now, ago(air_min), ago(ndw_min)) == expected


def test_health_query_leaves_out_backfill_runs(monkeypatch):
    seen = []
    monkeypatch.setattr(dashboard, "_query", lambda sql, params: seen.append(params) or [])
    assert dashboard.ingestion_summary() == {}
    assert seen == [(["Luchtmeetnet", "NDW"],)]


def test_health_database_down_keeps_the_contract_shape(client, monkeypatch):
    def down():
        raise dashboard.SourceUnavailable("database", "database unreachable")
    monkeypatch.setattr(dashboard, "ingestion_summary", down)
    resp = client.get("/health")
    assert resp.status_code == 503
    body = resp.json()
    assert body["status"] == "degraded" and body["error"] == "database unreachable"
    assert body["ndw"] == body["luchtmeetnet"] == {"last_successful_fetch": None,
                                                   "bad_data_count": None}
    assert body["checked_at"]


# --- / ---------------------------------------------------------------------------------

def test_index_page_fetches_the_four_sites_and_refreshes(client):
    resp = client.get("/")
    assert resp.status_code == 200 and resp.headers["content-type"].startswith("text/html")
    assert '"/site/" + site' in resp.text and "setInterval(tick, 60000)" in resp.text
    assert "if (busy) return;" in resp.text and "AbortController" in resp.text


def test_index_page_states_the_threshold_in_use(client):
    text = client.get("/").text
    assert f"minus {dashboard.THRESHOLD:g} &micro;g" in text and "__" not in text
