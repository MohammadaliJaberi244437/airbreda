"""Redis message broker for the ingestion scripts (course Day 2 messaging prototype).

Each reading written to sensor_readings is also pushed as a JSON message onto the Redis
list "readings", in the course format:
    {"station_id": ..., "timestamp": ISO8601, "component": ..., "value": ...}

Design: the database write is the source of truth and the broker is a best-effort side
channel. Publishing happens only after the rows are written, and a missing or broken
Redis never fails an ingestion run: the messages are dropped with a WARNING, the run
still succeeds, and the readings stay available in PostgreSQL. With REDIS_HOST unset
(how the VM runs) this module does nothing at all, so cloud behaviour is unchanged.
"""
import json
import logging
import os

import redis

from common import log_event

LIST_KEY = "readings"


def reading_message(station_id, timestamp, component, value):
    """One reading in the course message format (timestamp as an ISO 8601 string)."""
    return {"station_id": station_id, "timestamp": timestamp.isoformat(),
            "component": component, "value": value}


def messages_from_rows(rows):
    """Messages for sensor_readings tuples (station_id, timestamp, component, value, flag)."""
    return [reading_message(station_id, ts, component, value)
            for station_id, ts, component, value, *_ in rows]


def publish_readings(messages):
    """RPUSH each message as JSON onto the "readings" list; return how many were pushed.

    Returns 0 without doing anything when REDIS_HOST is unset or there are no messages,
    and returns 0 with a WARNING (never raises) when Redis cannot be reached.
    """
    host = os.getenv("REDIS_HOST")
    if not host or not messages:
        return 0
    port = int(os.getenv("REDIS_PORT") or 6379)
    payloads = [json.dumps(m, default=str) for m in messages]
    client = redis.Redis(host=host, port=port, socket_connect_timeout=3)
    try:
        client.rpush(LIST_KEY, *payloads)  # one round trip, one list element per message
    except (redis.RedisError, OSError) as exc:
        log_event(logging.WARNING, event="broker_unavailable", host=host, port=port,
                  list=LIST_KEY, messages_dropped=len(payloads), error=repr(exc))
        return 0
    finally:
        client.close()
    log_event(logging.INFO, event="readings_published", host=host, port=port,
              list=LIST_KEY, count=len(payloads))
    return len(payloads)
