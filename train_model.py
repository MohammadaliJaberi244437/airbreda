"""Fit NO2 ~ total traffic intensity + local hour (LinearRegression).

Reads training_data.csv, writes model.pkl (joblib) and model_meta.json.
Usage: python train_model.py
"""
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
import sklearn
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error, r2_score
from sklearn.model_selection import LeaveOneOut, cross_val_predict

from features import FEATURES

ROOT = Path(__file__).resolve().parent
DATA_PATH = ROOT / "training_data.csv"
MODEL_PATH = ROOT / "model.pkl"
META_PATH = ROOT / "model_meta.json"
TARGET = "no2_ug_m3"
MIN_ROWS_FOR_LOO = 10
N_PARAMS = len(FEATURES) + 1  # coefficients + intercept


def evaluate(model, X, y):
    """(metrics dict, evaluation_note). Undefined metrics are None, never NaN."""
    n = len(y)
    fitted = model.predict(X)
    metrics = {
        "r2_in_sample": float(r2_score(y, fitted)) if n >= 2 and np.ptp(y) > 0 else None,
        "mae_in_sample": float(mean_absolute_error(y, fitted)),
        "mae_leave_one_out": None,
    }
    if n >= MIN_ROWS_FOR_LOO:
        loo = cross_val_predict(LinearRegression(), X, y, cv=LeaveOneOut())
        metrics["mae_leave_one_out"] = float(mean_absolute_error(y, loo))
        note = (f"{n} rows: in-sample R2/MAE plus leave-one-out MAE (each hour predicted by a "
                "model fitted on all other hours). Neighbouring hours are correlated, so even "
                "the leave-one-out MAE is optimistic.")
    else:
        note = (f"Only {n} row(s): no train/test split and no cross-validation; R2 and MAE are "
                "IN-SAMPLE (measured on the rows the model was fitted on) and overstate accuracy.")
        if n <= N_PARAMS:
            note += (f" With n <= {N_PARAMS} the model has at least as many parameters as rows"
                     + (", so it reproduces the training rows exactly"
                        if metrics["mae_in_sample"] < 1e-6 else
                        " (it still misses some, because rows share the same inputs)")
                     + "; the metrics say nothing about predictive skill.")
    if metrics["r2_in_sample"] is None:
        note += " R2 is undefined (it needs at least 2 rows with different NO2 values)."
    return metrics, note


def sign_check(coef_traffic, X, y):
    """One sentence on the direction of the traffic effect, and how far to trust it."""
    if len(y) == 1:
        return "One row: no traffic effect can be estimated, the model predicts that row's NO2."
    if np.ptp(X[:, 0]) == 0:
        return ("No traffic effect estimated: every row has the same total traffic, so there "
                "is no variation to learn it from.")
    if np.ptp(y) == 0:
        return ("No traffic effect estimated: every row has the same NO2 value, so the model "
                "predicts that constant.")
    # n <= N_PARAMS: as many parameters as rows, so the fit is exact or minimum-norm and its
    # coefficients say nothing about the real effect.
    caveat = (f"Underdetermined fit (n = {len(y)} <= {N_PARAMS} parameters), the sign is "
              "arbitrary: " if len(y) <= N_PARAMS else "")
    per_1000 = coef_traffic * 1000
    if abs(coef_traffic) < 1e-9:
        return (caveat or "Zero coefficient: ") + "no traffic effect estimated."
    if coef_traffic > 0:
        return ((caveat or "Expected sign: ") + "more traffic raises predicted NO2 "
                f"(+{per_1000:.2f} ug/m3 per +1000 veh/h at the same hour).")
    return ((caveat or "UNEXPECTED sign: ") + "more traffic lowers predicted NO2 "
            f"({per_1000:.2f} ug/m3 per +1000 veh/h)."
            + ("" if caveat else " Likely too little data or confounding (wind, boundary layer "
               "height, time of day), not a real effect."))


def main():
    df = pd.read_csv(DATA_PATH)
    if df.empty:
        print(f"{DATA_PATH.name} has no rows: nothing to train on. Run build_training_data.py "
              "after more NO2 hours overlap with the traffic capture.", file=sys.stderr)
        return 1
    X = df[FEATURES].to_numpy(dtype=float)  # plain arrays: predict.py passes arrays too
    y = df[TARGET].to_numpy(dtype=float)
    model = LinearRegression().fit(X, y)
    metrics, note = evaluate(model, X, y)
    coefficients = {f: float(c) for f, c in zip(FEATURES, model.coef_)}
    sign = sign_check(coefficients["total_intensity_veh_per_hr"], X, y)

    meta = {
        "trained_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "n_rows": len(df),
        "time_range": {"start": df["timestamp"].min(), "end": df["timestamp"].max()},
        "features": FEATURES,
        "target": TARGET,
        "coefficients": coefficients,
        "intercept": float(model.intercept_),
        "metrics": metrics,
        "evaluation_note": note,
        "sign_check": sign,
        "sklearn_version": sklearn.__version__,
    }
    joblib.dump(model, MODEL_PATH)
    META_PATH.write_text(json.dumps(meta, indent=2) + "\n", encoding="utf-8")

    def fmt(value):
        return "null" if value is None else f"{value:.3f}"

    print(f"Rows: {len(df)}  ({meta['time_range']['start']} .. {meta['time_range']['end']})")
    print("Coefficients: " + ", ".join(f"{f} = {c:.6f}" for f, c in coefficients.items())
          + f", intercept = {model.intercept_:.3f}")
    print(f"Sign check: {sign}")
    print(f"R2 (in-sample): {fmt(metrics['r2_in_sample'])}   MAE (in-sample): "
          f"{fmt(metrics['mae_in_sample'])} ug/m3   MAE (leave-one-out): "
          f"{fmt(metrics['mae_leave_one_out'])}"
          + ("" if len(df) >= MIN_ROWS_FOR_LOO else f" (needs >= {MIN_ROWS_FOR_LOO} rows)"))
    print(f"Evaluation: {note}")
    print(f"Saved {MODEL_PATH.name} and {META_PATH.name} (scikit-learn {sklearn.__version__})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
