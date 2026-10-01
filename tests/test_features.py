"""Shared feature code: NO2 window labels, local hour of day and hourly means."""
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import pytest

from features import hour_of_day, hourly_site_windows, ndw_key_site, no2_window_label, \
    total_intensity, window_hour_of_day

UTC = timezone.utc


def at(hour, day=1, month=10, year=2026):
    return datetime(year, month, day, hour, tzinfo=UTC)


def test_ndw_sample_maps_to_the_no2_hour_that_ends_after_it():
    assert no2_window_label("2026-10-01T08:30:00Z") == at(9)
    assert no2_window_label("2026-10-01T08:00:00Z") == at(9)
    assert no2_window_label("2026-10-01T08:59:59+00:00") == at(9)
    # 10:30 in Amsterdam (CEST) is 08:30 UTC.
    amsterdam = ZoneInfo("Europe/Amsterdam")
    assert no2_window_label(datetime(2026, 10, 1, 10, 30, tzinfo=amsterdam)) == at(9)
    assert no2_window_label("2026-12-31T23:30:00Z") == datetime(2027, 1, 1, 0, tzinfo=UTC)


@pytest.mark.parametrize("ts, local_hour", [
    ("2026-07-01T08:30:00Z", 10),  # summer time, UTC+2
    ("2026-01-15T08:30:00Z", 9),   # winter time, UTC+1
    ("2026-03-29T00:30:00Z", 1),   # clocks go forward at 01:00 UTC
    ("2026-03-29T01:30:00Z", 3),   # 02:xx local does not exist that day
    ("2026-10-25T00:30:00Z", 2),   # clocks go back at 01:00 UTC
    ("2026-10-25T01:30:00Z", 2),   # 02:xx local happens twice
])
def test_hour_of_day_uses_amsterdam_local_time_including_dst(ts, local_hour):
    assert hour_of_day(ts) == local_hour


def test_naive_timestamps_are_rejected():
    with pytest.raises(ValueError):
        hour_of_day(datetime(2026, 10, 1, 8, 30))


def test_training_hour_is_the_hour_the_traffic_was_measured_in():
    # NO2 label 09:00Z covers 08:00-09:00Z = 10:00-11:00 CEST.
    assert window_hour_of_day(at(9)) == 10 == hour_of_day("2026-10-01T08:30:00Z")


def _row(site, minute, intensity, hour=8, lanes=None):
    return {"site": site, "measured_at": f"2026-10-01T{hour:02d}:{minute:02d}:00Z",
            "intensity_veh_per_hr": intensity, "lane_flows": intensity if lanes is None else lanes}


def _means(rows):
    return {label: {site: w.mean for site, w in sites.items()}
            for label, sites in hourly_site_windows(rows).items()}


def test_hourly_means_and_total_require_all_four_sites():
    rows = [_row("hrl", 30, "1200.0"), _row("hrl", 40, "1500.0"), _row("hrr", 30, "900.0"),
            _row("vwd", 31, "300.0"), _row("vwa", 35, "240.0"),
            _row("vwa", 45, ""),                # no valid intensity: skipped, not a zero
            _row("hrl", 5, "1000.0", hour=9)]   # next window, one site only
    means = _means(rows)
    assert means[at(9)] == {"hrl": 1350.0, "hrr": 900.0, "vwd": 300.0, "vwa": 240.0}
    assert total_intensity(means[at(9)]) == 2790.0
    assert means[at(10)] == {"hrl": 1000.0}
    assert total_intensity(means[at(10)]) is None


def test_a_lane_in_data_error_or_a_bad_time_invalidates_the_sample():
    rows = [_row("hrl", 10, "1500.0", lanes="700.0;800.0"),
            _row("hrl", 20, "1140.0", lanes="None;1140.0"),   # NDW dataError on one lane
            _row("hrl", 30, "1140.0", lanes="1140.0;"),
            _row("hrl", 40, "900.0", lanes="-1.0;901.0"),
            _row("hrl", 50, "1000.0", lanes=""),
            {**_row("hrl", 55, "1000.0"), "measured_at": "not a time"}]
    [window] = hourly_site_windows(rows)[at(9)].values()
    assert (window.mean, window.n) == (1500.0, 1)


@pytest.mark.parametrize("minutes, covered", [
    ((6, 16, 26, 36, 46, 56), True),
    ((6, 16, 26, 36), True),        # 3+ samples over 30 min: half the hour
    ((6, 16, 26), False),           # still filling: 20 min
    ((30, 31, 36, 41, 46, 51, 56), False),  # only the second half (26 min)
    ((5, 55), False),               # two samples are too few
])
def test_a_window_must_cover_half_its_hour(minutes, covered):
    window = hourly_site_windows([_row("vwd", m, "300.0") for m in minutes])[at(9)]["vwd"]
    assert window.covered is covered


def test_ndw_key_site_only_accepts_hourly_csv_keys():
    assert ndw_key_site("ndw/2026-10-01/08-hrl.csv") == "hrl"
    assert ndw_key_site("raw/ndw/2026-10-01/082500_meetgegevens.xml.gz") is None
    assert ndw_key_site("ndw/2026-10-01/08-xyz.csv") is None
