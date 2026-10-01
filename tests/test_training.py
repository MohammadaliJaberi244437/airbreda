"""build_training_data.join coverage rule and train_model's notes on tiny data."""
from datetime import datetime, timezone

import numpy as np
from sklearn.linear_model import LinearRegression

import build_training_data
import train_model
from features import SITE_LABELS

UTC = timezone.utc
LABEL_09, LABEL_10 = datetime(2026, 10, 1, 9, tzinfo=UTC), datetime(2026, 10, 1, 10, tzinfo=UTC)


def _rows(hour, minutes):
    return [{"site": s, "measured_at": f"2026-10-01T{hour:02d}:{m:02d}:00Z",
             "intensity_veh_per_hr": "500.0", "lane_flows": "500.0"}
            for s in SITE_LABELS for m in minutes]


def test_join_drops_hours_whose_traffic_covers_too_little_of_the_hour():
    rows = _rows(8, (30, 36, 41, 46, 51, 56)) + _rows(9, (6, 16, 26, 36, 46, 56))
    df, dropped = build_training_data.join({LABEL_09: 30.0, LABEL_10: 33.0}, set(), rows)
    assert list(df["timestamp"]) == [LABEL_10.isoformat()]
    assert df["n_samples"].iloc[0] == 24 and df["total_intensity_veh_per_hr"].iloc[0] == 2000.0
    [(reason, hours)] = dropped.items()
    assert reason.startswith("partial traffic coverage")
    assert hours == [(LABEL_09, "hrl 6 samples over 26 min, hrr 6 samples over 26 min, "
                                "vwd 6 samples over 26 min, vwa 6 samples over 26 min")]


def _note(X, y):
    X, y = np.array(X, dtype=float), np.array(y, dtype=float)
    return train_model.evaluate(LinearRegression().fit(X, y), X, y)[1]


def test_exact_fit_is_only_claimed_when_the_rows_are_reproduced():
    assert "reproduces the training rows exactly" in _note([[3000, 10], [3500, 11]], [30, 35])
    note = _note([[3000, 10], [3000, 10]], [30, 35])  # same inputs, different NO2
    assert "exactly" not in note and "share the same inputs" in note


def test_sign_check_names_the_real_reason():
    X = np.array([[3000, 10], [3000, 11], [3000, 12], [3000, 13], [3000, 14]], dtype=float)
    assert "same total traffic" in train_model.sign_check(0.0, X, np.arange(5.0))
    X[:, 0] = [3000, 3100, 3200, 3300, 3400]
    assert "same NO2 value" in train_model.sign_check(0.0, X, np.full(5, 30.0))
    two = train_model.sign_check(-0.02252, X[:2], np.array([30.0, 28.0]))
    assert two.startswith("Underdetermined fit") and "UNEXPECTED" not in two
    assert train_model.sign_check(0.002, X, np.arange(5.0)).startswith("Expected sign")
    assert train_model.sign_check(-0.002, X, np.arange(5.0)).startswith("UNEXPECTED sign")
