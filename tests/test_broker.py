"""Day 2 Redis broker. A fake client is monkeypatched in, so no real Redis is needed."""
import gzip
import json
import logging
from datetime import datetime, timezone
from types import SimpleNamespace

import pandas as pd
import pytest
import redis
import requests

import broker
import ingest_air
import ingest_traffic
from common import LocalStore
from ingest_traffic import SITES
from test_data_quality import FIXTURE  # the small DATEX II feed (hrl and vwd only)

TS = datetime(2026, 10, 1, 8, 0, tzinfo=timezone.utc)


class FakeRedis:
    """Records the constructor arguments and RPUSH calls of redis.Redis."""

    def __init__(self, **kwargs):
        self.kwargs, self.lists, self.closed = kwargs, {}, False

    def rpush(self, key, *values):
        self.lists.setdefault(key, []).extend(values)
        return len(self.lists[key])

    def close(self):
        self.closed = True


@pytest.fixture
def clients(monkeypatch):
    """REDIS_HOST set, redis.Redis replaced by FakeRedis; yields the clients created."""
    created = []

    def factory(**kwargs):
        created.append(FakeRedis(**kwargs))
        return created[-1]

    monkeypatch.setenv("REDIS_HOST", "redis")
    monkeypatch.delenv("REDIS_PORT", raising=False)
    monkeypatch.setattr(broker.redis, "Redis", factory)
    return created


@pytest.fixture
def no_redis(monkeypatch):
    """REDIS_HOST unset (how the VM runs); any attempt to build a client fails the test."""
    def factory(**kwargs):
        raise AssertionError("redis.Redis must not be created when REDIS_HOST is unset")

    monkeypatch.delenv("REDIS_HOST", raising=False)
    monkeypatch.setattr(broker.redis, "Redis", factory)


def _pushed(clients):
    return [json.loads(p) for c in clients for p in c.lists.get("readings", [])]


def _events(caplog, name):
    return [r for r in caplog.records if f'"event": "{name}"' in r.getMessage()]


# No-op when REDIS_HOST is unset.

def test_publish_is_a_noop_without_redis_host(no_redis, caplog):
    with caplog.at_level(logging.DEBUG):
        assert broker.publish_readings([broker.reading_message("NL10240", TS, "NO2", 18.4)]) == 0
    assert caplog.records == []  # not even a log line: VM output stays identical


def test_ingest_air_run_is_unchanged_without_redis_host(no_redis, monkeypatch):
    df = pd.DataFrame({"component": ["NO2", "NO2"], "value": [18.4, 21.0],
                       "timestamp": ["2026-10-01T07:00:00Z", "2026-10-01T08:00:00Z"]})
    monkeypatch.setattr(ingest_air, "fetch_no2", lambda: df)
    assert ingest_air.run(None) == 0


# Message format.

def test_reading_message_matches_the_course_format():
    message = broker.reading_message("NL10240", TS, "NO2", 18.4)
    assert message == {"station_id": "NL10240", "timestamp": "2026-10-01T08:00:00+00:00",
                       "component": "NO2", "value": 18.4}
    assert datetime.fromisoformat(message["timestamp"]) == TS  # valid ISO 8601


def test_messages_from_rows_drop_the_flag_and_keep_nulls():
    rows = [("NL10240", TS, "NO2", None, True), ("NL10240", TS, "NO2", 18.4, False)]
    assert broker.messages_from_rows(rows) == [
        {"station_id": "NL10240", "timestamp": TS.isoformat(), "component": "NO2", "value": None},
        {"station_id": "NL10240", "timestamp": TS.isoformat(), "component": "NO2", "value": 18.4},
    ]


def test_publish_pushes_one_json_message_per_reading(clients, caplog):
    messages = [broker.reading_message("NL10240", TS, "NO2", 18.4),
                broker.reading_message("NL10240", TS, "NO2", None)]
    with caplog.at_level(logging.INFO):
        assert broker.publish_readings(messages) == 2

    [client] = clients
    assert client.kwargs == {"host": "redis", "port": 6379, "socket_connect_timeout": 3}
    assert _pushed(clients) == messages  # in order, null stays null
    assert client.closed
    assert len(_events(caplog, "readings_published")) == 1


def test_publish_uses_redis_port(clients, monkeypatch):
    monkeypatch.setenv("REDIS_PORT", "6380")
    broker.publish_readings([broker.reading_message("NL10240", TS, "NO2", 1.0)])
    assert clients[0].kwargs["port"] == 6380


def test_publish_with_no_messages_does_not_connect(clients):
    assert broker.publish_readings([]) == 0
    assert clients == []


def test_ingest_air_publishes_every_row_written(clients, monkeypatch):
    df = pd.DataFrame({"component": ["NO2", "NO2", "NO2"], "value": [18.4, None, 21.0],
                       "timestamp": ["2026-10-01T08:00:00Z", "2026-10-01T07:00:00Z",
                                     "2026-10-01T06:00:00Z"]})
    monkeypatch.setattr(ingest_air, "fetch_no2", lambda: df)
    assert ingest_air.run(None) == 0
    assert _pushed(clients) == [
        {"station_id": "NL10240", "timestamp": f"2026-10-01T0{h}:00:00+00:00",
         "component": "NO2", "value": v} for h, v in ((6, 21.0), (7, None), (8, 18.4))]


def test_ingest_traffic_publishes_one_message_per_site_per_metric(clients, monkeypatch, tmp_path):
    def fake_fetch(url, **kwargs):
        if url == ingest_traffic.MEASUREMENTS_URL:
            return SimpleNamespace(content=gzip.compress(FIXTURE))
        raise requests.ConnectionError("config feed not needed here")

    monkeypatch.setattr(ingest_traffic, "fetch", fake_fetch)
    assert ingest_traffic.run_live(None, LocalStore(tmp_path)) == 0

    pushed = {(m["station_id"], m["component"]): m for m in _pushed(clients)}
    # hrl has a -1 lane speed, so only its intensity row is written (and published).
    assert set(pushed) == {(SITES["hrl"], "intensity"), (SITES["vwd"], "intensity"),
                           (SITES["vwd"], "speed")}
    assert pushed[(SITES["vwd"], "speed")] == {
        "station_id": SITES["vwd"], "timestamp": "2026-10-01T09:02:00+00:00",
        "component": "speed", "value": 31.0}


# Broker down: warn, return 0, never raise (the database write is the source of truth).

@pytest.mark.parametrize("error", [
    redis.exceptions.ConnectionError("Error 111 connecting to redis:6379. Connection refused."),
    redis.exceptions.TimeoutError("Timeout connecting to server"),
    OSError("Network is unreachable"),
])
def test_broker_down_does_not_raise(clients, monkeypatch, caplog, error):
    def rpush(self, key, *values):
        raise error

    monkeypatch.setattr(FakeRedis, "rpush", rpush)
    with caplog.at_level(logging.WARNING):
        assert broker.publish_readings([broker.reading_message("NL10240", TS, "NO2", 1.0)]) == 0

    [warning] = _events(caplog, "broker_unavailable")
    assert warning.levelno == logging.WARNING
    fields = json.loads(warning.getMessage())
    assert fields["host"] == "redis" and fields["messages_dropped"] == 1
    assert clients[0].closed


def test_ingest_air_run_succeeds_when_broker_is_down(clients, monkeypatch, caplog):
    def rpush(self, key, *values):
        raise redis.exceptions.ConnectionError("Connection refused")

    monkeypatch.setattr(FakeRedis, "rpush", rpush)
    df = pd.DataFrame({"component": ["NO2"], "value": [18.4],
                       "timestamp": ["2026-10-01T08:00:00Z"]})
    monkeypatch.setattr(ingest_air, "fetch_no2", lambda: df)
    with caplog.at_level(logging.INFO):
        assert ingest_air.run(None) == 0
    assert _events(caplog, "broker_unavailable")
    [recorded] = _events(caplog, "run_recorded")
    assert json.loads(recorded.getMessage())["success"] is True
