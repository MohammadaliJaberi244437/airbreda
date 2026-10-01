"""build_training_data.join coverage rule, train_model's notes on tiny data and the
logistic-vs-sigmoid risk comparison."""
import json
from datetime import datetime, timezone

import joblib
import numpy as np
import pandas as pd
import pytest
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import LeaveOneOut, cross_val_predict

import build_training_data
import train_model
from features import FEATURES, SITE_LABELS
from predict import exceedance_risk

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


# --- logistic regression and the risk comparison --------------------------------------

EXISTING_META_KEYS = {"trained_at", "n_rows", "time_range", "features", "target", "coefficients",
                      "intercept", "metrics", "evaluation_note", "sign_check", "sklearn_version"}


def _frame(no2):
    """Training rows with the given NO2 values; traffic rises row by row."""
    n = len(no2)
    return pd.DataFrame({
        "timestamp": [f"2026-10-{1 + i // 24:02d}T{i % 24:02d}:00:00+00:00" for i in range(n)],
        "no2_ug_m3": no2,
        "total_intensity_veh_per_hr": np.linspace(2000.0, 5000.0, n),
        "hour_of_day": [(i + 2) % 24 for i in range(n)],
    })


def _both_classes(n=30, noise=3.0):
    """NO2 rising with traffic from threshold - 15 to threshold + 15, plus Gaussian noise of
    `noise` ug/m3 (0 makes the two classes separable by traffic)."""
    noise = np.random.default_rng(0).normal(0, noise, n)
    return _frame(train_model.THRESHOLD + np.linspace(-15.0, 15.0, n) + noise)


def _train(df):
    model, logit, meta = train_model.train(df)
    json.dumps(meta, allow_nan=False)  # what main() writes: strict JSON, no NaN
    return model, logit, meta


def test_logistic_is_fitted_when_both_classes_are_present():
    df = _both_classes()
    model, logit, meta = _train(df)
    rc = meta["risk_comparison"]
    assert logit is not None and rc["logistic"]["fitted"] and rc["logistic"]["skip_reason"] is None
    assert 0 < rc["n_exceeded"] < 30 and len(rc["rows"]) == 30
    probabilities = [r["logistic_probability"] for r in rc["rows"]]
    assert all(0 <= p <= 1 for p in probabilities)
    assert rc["logistic"]["coefficients_log_odds"]["total_intensity_veh_per_hr"] > 0
    assert rc["in_sample"]["logistic"]["exceedances_caught"] > 0
    # The regression risk is predict.exceedance_risk of the clamped prediction, not a copy.
    X = df[FEATURES].to_numpy(dtype=float)
    for row, raw in zip(rc["rows"], model.predict(X)):
        assert row["regression_sigmoid_risk"] == round(exceedance_risk(max(0.0, raw)), 4)
        assert row["exceeded"] == (row["no2_ug_m3"] > train_model.THRESHOLD)


def test_logistic_coefficients_are_reported_in_raw_units():
    df = _both_classes()
    _, logit, meta = _train(df)
    logistic = meta["risk_comparison"]["logistic"]
    X = df[FEATURES].to_numpy(dtype=float)
    z = X @ np.array(list(logistic["coefficients_log_odds"].values())) \
        + logistic["intercept_log_odds"]
    assert np.allclose(1 / (1 + np.exp(-z)), logit.predict_proba(X)[:, 1])


def test_logistic_is_skipped_without_exceedance_hours():
    _, logit, meta = _train(_frame(train_model.THRESHOLD - 20 + np.linspace(0.0, 5.0, 30)))
    rc = meta["risk_comparison"]
    assert logit is None and rc["n_exceeded"] == 0
    assert rc["logistic"]["skip_reason"].startswith("no exceedance hours")
    assert rc["logistic"]["file"] is None and rc["in_sample"]["logistic"] is None
    assert all(r["logistic_probability"] is None for r in rc["rows"])
    assert all(0 <= r["regression_sigmoid_risk"] < 0.5 for r in rc["rows"])
    assert "not fitted (no exceedance hours" in rc["comparison_note"]


def test_logistic_is_skipped_when_every_hour_exceeds():
    _, logit, meta = _train(_frame(train_model.THRESHOLD + 10 + np.linspace(0.0, 5.0, 30)))
    assert logit is None
    assert "all exceed" in meta["risk_comparison"]["logistic"]["skip_reason"]


def test_logistic_is_skipped_with_too_few_rows():
    _, logit, meta = _train(_frame(train_model.THRESHOLD + np.array([-10.0, -5, 0, 5, 10])))
    assert logit is None
    rc = meta["risk_comparison"]
    assert rc["logistic"]["skip_reason"].startswith("too few rows")
    assert rc["in_sample"]["regression_sigmoid"]["brier_leave_one_out"] is None  # n < 10
    assert "leave-one-out" not in rc["comparison_note"]


def test_comparison_note_follows_the_numbers():
    few = _train(_both_classes(30))[2]["risk_comparison"]
    assert few["n_exceeded"] < train_model.MIN_EVENTS
    assert "cannot learn a reliable boundary" in few["comparison_note"]
    assert "in-sample Brier score is flattered" in few["comparison_note"]
    assert "sigmoid on the regression is the more trustworthy" in few["comparison_note"]
    many = _train(_both_classes(80))[2]["risk_comparison"]
    assert min(many["n_exceeded"], 80 - many["n_exceeded"]) >= train_model.MIN_EVENTS
    assert "enough examples" in many["comparison_note"] and "held-out" in many["comparison_note"]
    for rc in (few, many):
        assert rc["comparison_note"].isascii()
        for s in rc["in_sample"].values():  # both leave-one-out scores exist from 10 rows
            assert 0 <= s["brier_leave_one_out"] <= 1
            assert f"leave-one-out {s['brier_leave_one_out']:.3f}" in rc["comparison_note"]


def test_verdict_follows_the_leave_one_out_brier_score_not_the_in_sample_one():
    """Noise of 8 ug/m3 is about the spread the sigmoid's steepness (0.2 per ug/m3) assumes,
    so there the logistic model wins in-sample (it was fitted to these labels) yet loses once
    every hour is scored by a model that never saw it. Separable data keeps it ahead."""
    noisy = _train(_both_classes(80, noise=8.0))[2]["risk_comparison"]
    sig, logi = noisy["in_sample"]["regression_sigmoid"], noisy["in_sample"]["logistic"]
    assert min(noisy["n_exceeded"], 80 - noisy["n_exceeded"]) >= train_model.MIN_EVENTS
    assert logi["brier_in_sample"] < sig["brier_in_sample"]
    assert logi["brier_leave_one_out"] > sig["brier_leave_one_out"]
    assert "sigmoid on the regression stays the more trustworthy" in noisy["comparison_note"]
    assert "better-founded" not in noisy["comparison_note"]
    separable = _train(_both_classes(80, noise=0.0))[2]["risk_comparison"]
    sig, logi = separable["in_sample"]["regression_sigmoid"], separable["in_sample"]["logistic"]
    assert logi["brier_leave_one_out"] < sig["brier_leave_one_out"]
    assert "logistic probability is the better-founded risk" in separable["comparison_note"]
    assert "leave-one-out Brier score is lower" in separable["comparison_note"]
    for rc in (noisy, separable):
        note = rc["comparison_note"]
        assert "leave-one-out scores decide" in note and "held-out" in note and note.isascii()


def test_leave_one_out_sigmoid_scores_each_hour_with_a_regression_that_never_saw_it():
    df = _both_classes(40)
    rc = _train(df)[2]["risk_comparison"]
    X, y = df[FEATURES].to_numpy(dtype=float), df["no2_ug_m3"].to_numpy(dtype=float)
    loo = cross_val_predict(LinearRegression(), X, y, cv=LeaveOneOut())
    risk = np.array([exceedance_risk(max(0.0, v)) for v in loo])
    expected = round(float(np.mean((risk - (y > train_model.THRESHOLD)) ** 2)), 4)
    assert rc["in_sample"]["regression_sigmoid"]["brier_leave_one_out"] == expected
    assert expected != rc["in_sample"]["regression_sigmoid"]["brier_in_sample"]


def test_leave_one_out_logistic_needs_two_hours_in_the_rarer_class():
    """12 rows with one exceedance: the logistic model is fitted and the regression gets a
    leave-one-out score, but the logistic fold without the lone exceedance would see one class."""
    no2 = np.append(train_model.THRESHOLD - 10 + np.linspace(0.0, 5.0, 11),
                    train_model.THRESHOLD + 5)
    _, logit, meta = _train(_frame(no2))
    rc = meta["risk_comparison"]
    assert logit is not None and rc["n_exceeded"] == 1
    assert rc["in_sample"]["regression_sigmoid"]["brier_leave_one_out"] is not None
    assert rc["in_sample"]["logistic"]["brier_leave_one_out"] is None
    note = rc["comparison_note"]
    assert "cannot learn a reliable boundary" in note and note.count("leave-one-out") == 1


@pytest.fixture
def paths(tmp_path, monkeypatch):
    for name in ("DATA_PATH", "MODEL_PATH", "LOGISTIC_PATH", "META_PATH"):
        monkeypatch.setattr(train_model, name, tmp_path / getattr(train_model, name).name)
    return train_model


def test_main_with_one_row_keeps_the_schema_and_removes_a_stale_logistic_model(paths, capsys):
    _frame([22.68]).to_csv(paths.DATA_PATH, index=False)
    paths.LOGISTIC_PATH.write_bytes(b"stale")
    assert paths.main() == 0
    meta = json.loads(paths.META_PATH.read_text(encoding="utf-8"))
    assert EXISTING_META_KEYS < set(meta) and meta["n_rows"] == 1
    assert joblib.load(paths.MODEL_PATH).predict([[3000.0, 8.0]])[0] == pytest.approx(22.68)
    assert not paths.LOGISTIC_PATH.exists()
    [row] = meta["risk_comparison"]["rows"]
    assert row["logistic_probability"] is None and 0 < row["regression_sigmoid_risk"] < 0.5
    sig = meta["risk_comparison"]["in_sample"]["regression_sigmoid"]
    assert sig["brier_leave_one_out"] is None and sig["brier_in_sample"] > 0
    assert "skipped, too few rows" in capsys.readouterr().out


def test_main_with_many_rows_saves_the_logistic_model(paths, capsys):
    _both_classes().to_csv(paths.DATA_PATH, index=False)
    assert paths.main() == 0
    meta = json.loads(paths.META_PATH.read_text(encoding="utf-8"))
    assert meta["risk_comparison"]["logistic"]["file"] == paths.LOGISTIC_PATH.name
    proba = joblib.load(paths.LOGISTIC_PATH).predict_proba([[3000.0, 8.0], [5000.0, 8.0]])[:, 1]
    assert 0 <= proba[0] < proba[1] <= 1
    assert "fitted" in capsys.readouterr().out
