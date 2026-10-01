"""Data-quality rules for both sources. No DB, S3 or network needed."""
import logging

import pandas as pd

from ingest_air import STALE_RUN, UPSERT_SQL, flag_bad_readings, to_db_rows
from ingest_traffic import SITES, SiteReading, build_traffic_rows, csv_key, merge_csv, parse_sites


def _events(caplog, name):
    return [r.getMessage() for r in caplog.records if f'"event": "{name}"' in r.getMessage()]


def _page(first_hour, values):
    """A Luchtmeetnet page, newest first: hour offsets from 2026-09-20T00:00Z."""
    hours = list(range(first_hour, first_hour + len(values)))[::-1]
    return pd.DataFrame({
        "component": ["NO2"] * len(values),
        "value": list(values)[::-1],
        "timestamp": [(pd.Timestamp("2026-09-20T00:00:00Z") + pd.Timedelta(hours=h)).isoformat()
                      for h in hours],
    })


def _upsert(old, new):
    """Python mirror of UPSERT_SQL's DO UPDATE: a revised value takes the fresh flag, an
    unchanged value keeps a flag it already has (IS DISTINCT FROM treats NULL = NULL)."""
    if old is None:
        return new
    (old_value, old_flag), (value, flag) = old, new
    return (value, flag if old_value != value else (old_flag or flag))


# (a) Luchtmeetnet: stale or null readings are written, flagged, never dropped.

def test_stale_and_null_air_readings_are_kept_and_flagged(caplog):
    # Newest first, as the API returns it.
    df = pd.DataFrame({
        "component": ["NO2"] * 6,
        "value": [12.0, None, 30.0, 30.0, 30.0, 25.0],
        "timestamp": [f"2026-10-01T{h:02d}:00:00+00:00" for h in (5, 4, 3, 2, 1, 0)],
    })
    with caplog.at_level(logging.WARNING):
        flagged, bad_count = flag_bad_readings(df)
    rows = to_db_rows(flagged)

    assert len(rows) == 6  # nothing dropped
    by_hour = {ts.hour: (value, is_flagged) for _, ts, _, value, is_flagged in rows}
    assert by_hour[4] == (None, True)  # null stored as SQL NULL, not NaN
    assert all(by_hour[h] == (30.0, True) for h in (1, 2, 3))  # whole stale run flagged
    assert by_hour[0] == (25.0, False) and by_hour[5] == (12.0, False)
    assert bad_count == 4
    assert len(_events(caplog, "DATA_QUALITY_ERROR")) == 4


def test_repeated_value_across_a_gap_is_not_stale():
    df = pd.DataFrame({
        "component": ["NO2"] * 3,
        "value": [30.0, 30.0, 30.0],
        "timestamp": ["2026-10-01T00:00:00Z", "2026-10-01T01:00:00Z", "2026-10-01T05:00:00Z"],
    })
    flagged, bad_count = flag_bad_readings(df)
    assert bad_count == 0 and not flagged["is_flagged"].any()


def test_stale_flags_survive_the_page_edge_through_the_sticky_upsert():
    # Hours 0, 1, 2 are frozen at 30.0. Each hourly run re-fetches the newest 50 hours, so
    # one hour later the oldest frozen hour is off the page and the remaining two no longer
    # form a run of STALE_RUN: judged by that page alone they are unflagged.
    frozen = [30.0] * STALE_RUN
    values = lambda start: [30.0 if h < STALE_RUN else 20.0 + h % 7 for h in range(start, start + 50)]  # noqa: E731
    pages = [flag_bad_readings(_page(start, values(start)))[0] for start in range(STALE_RUN)]
    flags_by_page = [[bool(f) for f in p.loc[p["value"] == 30.0, "is_flagged"]] for p in pages]
    assert flags_by_page == [[True] * 3, [False] * 2, [False]]

    # UPSERT_SQL keeps the flag while the value is unchanged, so after the three runs every
    # frozen hour is still flagged; a later revision of the value would get a fresh flag.
    table = {}
    for page in pages:
        for _, ts, _, value, flag in to_db_rows(page):
            table[ts] = _upsert(table.get(ts), (value, flag))
    assert [table[ts] for ts in sorted(table) if table[ts][0] == 30.0] == [(30.0, True)] * len(frozen)
    assert _upsert((30.0, True), (31.5, False)) == (31.5, False)
    assert _upsert((None, True), (None, True)) == (None, True)
    assert "IS DISTINCT FROM EXCLUDED.value" in UPSERT_SQL
    assert "sensor_readings.is_flagged OR EXCLUDED.is_flagged" in UPSERT_SQL


def test_only_readings_newer_than_the_previous_run_are_counted(caplog):
    # An 11-hour null outage at hours 5 to 15 stays inside the 50-hour page for two days.
    # Every row is still written with its flag, but the count (ingestion_runs.bad_data_count,
    # /health, BAD_DATA_THRESHOLD_EXCEEDED) covers only the hours new to this run.
    values = [None if 5 <= h <= 15 else 20.0 + h % 7 for h in range(50)]
    previous_last = pd.Timestamp("2026-09-20T00:00:00Z") + pd.Timedelta(hours=48)
    with caplog.at_level(logging.WARNING):
        flagged, bad_count = flag_bad_readings(_page(0, values), since=previous_last.to_pydatetime())
    assert int(flagged["is_flagged"].sum()) == 11 and bad_count == 0
    assert _events(caplog, "DATA_QUALITY_ERROR") == []

    values[49] = None  # the hour that is new to this run is null
    with caplog.at_level(logging.WARNING):
        flagged, bad_count = flag_bad_readings(_page(0, values), since=previous_last)
    assert int(flagged["is_flagged"].sum()) == 12 and bad_count == 1
    assert len(_events(caplog, "DATA_QUALITY_ERROR")) == 1
    assert flag_bad_readings(_page(0, values))[1] == 12  # first run or dry-run: the whole page


# (b) NDW: a -1 lane speed means no speed row, but the intensity row stays.

def test_ndw_minus_one_speed_skips_speed_row_and_counts_bad(caplog):
    broken = SiteReading("hrl", SITES["hrl"], "2026-10-01T08:31:00Z", [600.0, 900.0], [-1.0, 95.0])
    healthy = SiteReading("vwd", SITES["vwd"], "2026-10-01T08:31:00Z", [180.0], [31.0])
    with caplog.at_level(logging.WARNING):
        rows, bad_count = build_traffic_rows([broken, healthy])

    components = {(station, comp): (value, flag) for station, _, comp, value, flag in rows}
    assert (SITES["hrl"], "speed") not in components
    assert components[(SITES["hrl"], "intensity")] == (1500.0, False)
    assert components[(SITES["vwd"], "speed")] == (31.0, False)
    assert components[(SITES["vwd"], "intensity")] == (180.0, False)
    assert bad_count == 1
    assert all(value != -1 for _, _, _, value, _ in rows)  # no sentinel ever written
    [warning] = _events(caplog, "DATA_QUALITY_ERROR")
    assert '"field": "speed"' in warning and '"value": -1.0' in warning


def test_backfill_csv_row_applies_the_same_rule():
    row = {"measured_at": "2026-10-01T08:30:00Z", "site": "hrr", "site_id": SITES["hrr"],
           "intensity_veh_per_hr": "900.0", "speed_kmh": "101.5", "lane_flows": "400.0;500.0",
           "lane_speeds": "103.0;-1.0", "speed_invalid_count": "1",
           "captured_at": "2026-10-01T08:33:38+00:00"}
    rows, bad_count = build_traffic_rows([SiteReading.from_csv(row)])
    assert [comp for _, _, comp, _, _ in rows] == ["intensity"]
    assert bad_count == 1


# (c) DATEX II parsing on a small inline fixture.

def _quantity(index, kind, value, data_error=False):
    tag, wrapper = ("vehicleFlowRate", "vehicleFlow") if kind == "TrafficFlow" \
        else ("speed", "averageVehicleSpeed")
    error = "<com:dataError>true</com:dataError>" if data_error else ""
    inner = f"<com:{tag}>{value}</com:{tag}>" if value is not None else ""
    return (f'<roa:physicalQuantity index="{index}"><roa:physicalQuantity '
            f'xsi:type="roa:SinglePhysicalQuantity"><roa:basicData xsi:type="roa:{kind}">'
            f'<roa:{wrapper} accuracy="0.0">{error}{inner}</roa:{wrapper}></roa:basicData>'
            f'</roa:physicalQuantity></roa:physicalQuantity>')


def _site(site_id, time, quantities):
    return (f'<roa:siteMeasurements><roa:measurementSiteReference targetClass="roa:MeasurementSite" '
            f'id="{site_id}" version="1"/>{"".join(quantities)}<roa:measurementTimeDefault>'
            f'<roa:timeValue>{time}</roa:timeValue></roa:measurementTimeDefault></roa:siteMeasurements>')


FIXTURE = (
    '<?xml version="1.0" encoding="UTF-8"?><mc:messageContainer><mc:payload>'
    + _site("RWS01_MONIBAS_9999xxx0000ra", "2026-10-01T08:31:00Z",
            [_quantity(1, "TrafficFlow", 9999)])  # not one of ours
    + _site(SITES["hrl"], "2026-10-01T08:31:00Z", [
        _quantity(1, "TrafficFlow", 1020), _quantity(2, "TrafficFlow", 1260),
        _quantity(3, "TrafficSpeed", "107.0"), _quantity(4, "TrafficSpeed", "-1.0")])
    + _site(SITES["vwd"], "2026-10-01T09:02:00Z", [
        _quantity(1, "TrafficFlow", None),             # value missing entirely
        _quantity(2, "TrafficFlow", 0, data_error=True),  # NDW placeholder, not a real 0
        _quantity(3, "TrafficFlow", 180), _quantity(4, "TrafficSpeed", "31.0")])
    + "</mc:payload></mc:messageContainer>"
).encode()


def test_parse_datex_fixture(caplog):
    with caplog.at_level(logging.WARNING):
        sites = {s.label: s for s in parse_sites(FIXTURE, "2026-10-01T09:05:00+00:00")}

    assert set(sites) == {"hrl", "vwd"}
    assert len(_events(caplog, "site_missing")) == 2  # hrr and vwa are not in the fixture

    hrl = sites["hrl"]
    assert hrl.measured_at == "2026-10-01T08:31:00Z"
    assert hrl.lane_flows == [1020.0, 1260.0] and hrl.lane_speeds == [107.0, -1.0]
    assert hrl.intensity_veh_per_hr == 2280.0 and hrl.speed_kmh == 107.0
    assert hrl.speed_invalid_count == 1

    vwd = sites["vwd"]
    assert vwd.lane_flows == [None, None, 180.0]  # missing and dataError lanes are not counted
    assert vwd.intensity_veh_per_hr == 180.0 and vwd.speed_kmh == 31.0
    assert csv_key(vwd) == "ndw/2026-10-01/09-vwd.csv"  # measurement hour, not the clock


def test_csv_merge_appends_once_per_measurement():
    reading = SiteReading("hrl", SITES["hrl"], "2026-10-01T08:31:00Z", [360.0], [109.0], "x")
    first, added = merge_csv(None, [reading.csv_row()])
    assert added == 1
    second, added = merge_csv(first, [reading.csv_row()])
    assert added == 0 and second == first
    reading.measured_at = "2026-10-01T08:41:00Z"
    third, added = merge_csv(first, [reading.csv_row()])
    assert added == 1 and third.decode().count("\n") == 3
