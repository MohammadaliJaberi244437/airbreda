"""Fit NO2 ~ total traffic intensity + local hour (LinearRegression), then compare two
exceedance risk scores: the dashboard's sigmoid on the predicted NO2 (predict.py) and,
when the data allow it, a LogisticRegression on exceeded = NO2 > NO2_THRESHOLD. Both are
scored in-sample and, from MIN_ROWS_FOR_LOO rows, leave-one-out; the leave-one-out Brier
score decides which one the comparison note recommends.

Reads training_data.csv, writes model.pkl (joblib), model_meta.json and, only when the
logistic model is fitted, model_logistic.pkl (a stale one from an earlier run is removed).
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
from sklearn.linear_model import LinearRegression, LogisticRegression
from sklearn.metrics import mean_absolute_error, r2_score
from sklearn.model_selection import LeaveOneOut, cross_val_predict
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

from features import FEATURES
from predict import STEEPNESS, THRESHOLD, exceedance_risk

ROOT = Path(__file__).resolve().parent
DATA_PATH = ROOT / "training_data.csv"
MODEL_PATH = ROOT / "model.pkl"
LOGISTIC_PATH = ROOT / "model_logistic.pkl"
META_PATH = ROOT / "model_meta.json"
TARGET = "no2_ug_m3"
MIN_ROWS_FOR_LOO = 10
MIN_ROWS_FOR_LOGISTIC = 10
# Rule of thumb (Peduzzi et al., 1996): about 10 hours of the rarer class per feature before
# logistic coefficients are stable.
MIN_EVENTS = 10 * len(FEATURES)
N_PARAMS = len(FEATURES) + 1  # coefficients + intercept


def loo_predictions(X, y):
    """Leave-one-out regression predictions (each hour predicted by a model fitted on all
    other hours), or None below MIN_ROWS_FOR_LOO."""
    if len(y) < MIN_ROWS_FOR_LOO:
        return None
    return cross_val_predict(LinearRegression(), X, y, cv=LeaveOneOut())


def evaluate(model, X, y, loo=None):
    """(metrics dict, evaluation_note). Undefined metrics are None, never NaN.
    loo: loo_predictions(X, y), passed in when the caller already has them."""
    n = len(y)
    if loo is None:
        loo = loo_predictions(X, y)
    fitted = model.predict(X)
    metrics = {
        "r2_in_sample": float(r2_score(y, fitted)) if n >= 2 and np.ptp(y) > 0 else None,
        "mae_in_sample": float(mean_absolute_error(y, fitted)),
        "mae_leave_one_out": None if loo is None else float(mean_absolute_error(y, loo)),
    }
    if loo is not None:
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


def logistic_pipeline():
    # Scaling lets the L2 penalty treat veh/h and hours alike and lets lbfgs converge.
    return Pipeline([("scale", StandardScaler()), ("logit", LogisticRegression(max_iter=1000))])


def fit_logistic(X, exceeded):
    """(fitted pipeline, None), or (None, why it was skipped)."""
    n, n_exceeded = len(exceeded), int(exceeded.sum())
    if n < MIN_ROWS_FOR_LOGISTIC:
        return None, f"too few rows: {n} < {MIN_ROWS_FOR_LOGISTIC}"
    if n_exceeded == 0:
        return None, f"no exceedance hours in the data (none above {THRESHOLD:g} ug/m3)"
    if n_exceeded == n:
        return None, f"no hours at or below {THRESHOLD:g} ug/m3 in the data (all exceed it)"
    return logistic_pipeline().fit(X, exceeded.astype(int)), None


def loo_logistic_proba(X, exceeded):
    """Leave-one-out P(exceeded), each hour scored by a logistic pipeline fitted on all other
    hours, or None below MIN_ROWS_FOR_LOO or when the rarer class has a single hour (the fold
    that leaves it out would see one class)."""
    n, n_exceeded = len(exceeded), int(exceeded.sum())
    if n < MIN_ROWS_FOR_LOO or min(n_exceeded, n - n_exceeded) < 2:
        return None
    return cross_val_predict(logistic_pipeline(), X, exceeded.astype(int), cv=LeaveOneOut(),
                             method="predict_proba")[:, 1]


def logistic_coefficients(pipe):
    """(coefficients, intercept) in log-odds per raw feature unit, with the scaling undone."""
    scale, logit = pipe.named_steps["scale"], pipe.named_steps["logit"]
    coef = logit.coef_[0] / scale.scale_
    return ({f: float(c) for f, c in zip(FEATURES, coef)},
            float(logit.intercept_[0] - coef @ scale.mean_))


def sigmoid_scores(predicted):
    """predict.exceedance_risk of each predicted NO2, clamped at 0 first as predict.predict is."""
    return np.array([exceedance_risk(float(v)) for v in np.maximum(predicted, 0.0)])


def brier(scores, exceeded):
    return round(float(np.mean((scores - exceeded.astype(float)) ** 2)), 4)


def score_summary(scores, exceeded, loo_scores=None):
    """Brier scores (in-sample and, when available, leave-one-out), hours flagged at >= 0.5
    and the range of a 0-1 risk score."""
    flagged = scores >= 0.5
    return {"brier_in_sample": brier(scores, exceeded),
            "brier_leave_one_out": None if loo_scores is None else brier(loo_scores, exceeded),
            "exceedances_caught": int(np.sum(flagged & exceeded)),
            "false_alarms": int(np.sum(flagged & ~exceeded)),
            "min": round(float(scores.min()), 4), "max": round(float(scores.max()), 4)}


def comparison_note(n, n_exceeded, skip_reason, sig, logi):
    """One paragraph, generated from the numbers: which risk score to trust here, and why."""
    def describe(s):
        span = (f"{s['min']:.2f}" if f"{s['min']:.2f}" == f"{s['max']:.2f}"
                else f"{s['min']:.2f} to {s['max']:.2f}")
        caught = (f"catches {s['exceedances_caught']} of {n_exceeded} exceedance hour(s) with"
                  if n_exceeded else "raises")
        scores = f"in-sample Brier {s['brier_in_sample']:.3f}"
        if s["brier_leave_one_out"] is not None:
            scores += f", leave-one-out {s['brier_leave_one_out']:.3f}"
        return f"gives {span} ({scores}); at >= 0.5 it {caught} {s['false_alarms']} false alarm(s)"

    text = (f"{n_exceeded} of {n} training hour(s) exceed {THRESHOLD:g} ug/m3. The sigmoid on "
            f"the regression's predicted NO2 {describe(sig)}. ")
    text += (f"The logistic model was not fitted ({skip_reason}). " if logi is None
             else f"The logistic probability {describe(logi)}. ")
    minority = min(n_exceeded, n - n_exceeded)
    if logi is not None and minority >= MIN_EVENTS:
        # minority >= MIN_EVENTS gives n >= 2 * MIN_EVENTS >= MIN_ROWS_FOR_LOO and at least two
        # hours per class, so both leave-one-out scores exist.
        sig_loo, logi_loo = sig["brier_leave_one_out"], logi["brier_leave_one_out"]
        if logi_loo < sig_loo:
            verdict = ("The logistic probability is the better-founded risk here: unlike the "
                       f"sigmoid, whose steepness ({STEEPNESS:g} per ug/m3) is chosen, it is "
                       "fitted to the observed exceedances, and its leave-one-out Brier score "
                       f"is lower ({logi_loo:.3f} against {sig_loo:.3f}).")
        else:
            verdict = ("The sigmoid on the regression stays the more trustworthy score: the "
                       "logistic model, which learns only from the yes/no labels, does not beat "
                       f"it out of sample (leave-one-out Brier {logi_loo:.3f} against "
                       f"{sig_loo:.3f}).")
        return text + (f"With {minority} hours in the rarer class the logistic model has enough "
                       f"examples (about 10 per feature) to learn a boundary. {verdict} The "
                       "leave-one-out scores decide, because the logistic model's in-sample "
                       "Brier score is flattered: it was fitted to exactly these labels. "
                       "Neighbouring hours are correlated, so even leave-one-out is optimistic; "
                       "confirm the choice on a held-out later period (a time-based split).")
    if logi is None:
        why = ("A logistic model learns only from the yes/no label, so it needs both outcomes "
               "and enough rows before it can place a boundary.")
    else:
        rate = n_exceeded / n
        why = (f"With only {minority} hour(s) in the rarer class (a common rule of thumb asks "
               f"for about {MIN_EVENTS}, 10 per feature) the logistic model cannot learn a "
               f"reliable boundary: its coefficients rest on {minority} example(s) of the rarer "
               "outcome and its in-sample Brier score is flattered because it was fitted to "
               "exactly these labels"
               + (f"; its probabilities barely move away from the base rate of {rate:.2f}."
                  if logi["max"] - logi["min"] < 0.3 else "."))
    return text + why + (
        " The sigmoid on the regression is the more trustworthy score here: it uses every "
        "measured NO2 value rather than a yes/no label, rises smoothly with predicted NO2 (0.5 "
        f"exactly where the prediction reaches {THRESHOLD:g} ug/m3) and is explained by the "
        "regression's coefficients. It is still a score, not a calibrated probability: its "
        f"steepness ({STEEPNESS:g} per ug/m3) is chosen, not fitted, and it is only as good as "
        f"a regression on {n} hour(s).")


def risk_comparison(df, X, regression, logit, skip_reason, loo=None):
    """The risk_comparison block of model_meta.json: both risk scores for every training row,
    scored in-sample and (from MIN_ROWS_FOR_LOO rows) leave-one-out.
    loo: loo_predictions(X, y), passed in when the caller already has them."""
    y = df[TARGET].to_numpy(dtype=float)
    exceeded = y > THRESHOLD
    if loo is None:
        loo = loo_predictions(X, y)
    predicted = np.maximum(regression.predict(X), 0.0)  # clamped as in predict.predict
    sigmoid = sigmoid_scores(predicted)
    sig = score_summary(sigmoid, exceeded, None if loo is None else sigmoid_scores(loo))
    proba = None if logit is None else logit.predict_proba(X)[:, 1]
    logi = (None if proba is None
            else score_summary(proba, exceeded, loo_logistic_proba(X, exceeded)))
    coefficients, intercept = (None, None) if logit is None else logistic_coefficients(logit)
    rows = [{"timestamp": rec["timestamp"], TARGET: rec[TARGET], "exceeded": bool(exceeded[i]),
             **{f: rec[f] for f in FEATURES},
             "no2_ug_m3_predicted": round(float(predicted[i]), 2),
             "regression_sigmoid_risk": round(float(sigmoid[i]), 4),
             "logistic_probability": None if proba is None else round(float(proba[i]), 4)}
            for i, rec in enumerate(df.to_dict("records"))]
    n_exceeded = int(exceeded.sum())
    return {
        "threshold_ug_m3": THRESHOLD,
        "n_exceeded": n_exceeded,
        "logistic": {"fitted": logit is not None, "skip_reason": skip_reason,
                     "file": None if logit is None else LOGISTIC_PATH.name,
                     "coefficients_log_odds": coefficients, "intercept_log_odds": intercept},
        "in_sample": {"regression_sigmoid": sig, "logistic": logi},
        "comparison_note": comparison_note(len(df), n_exceeded, skip_reason, sig, logi),
        "rows": rows,
    }


def train(df):
    """(regression, logistic pipeline or None, model_meta.json contents) for training rows."""
    X = df[FEATURES].to_numpy(dtype=float)  # plain arrays: predict.py passes arrays too
    y = df[TARGET].to_numpy(dtype=float)
    model = LinearRegression().fit(X, y)
    loo = loo_predictions(X, y)
    metrics, note = evaluate(model, X, y, loo)
    coefficients = {f: float(c) for f, c in zip(FEATURES, model.coef_)}
    logit, skip_reason = fit_logistic(X, y > THRESHOLD)
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
        "sign_check": sign_check(coefficients["total_intensity_veh_per_hr"], X, y),
        "sklearn_version": sklearn.__version__,
        "risk_comparison": risk_comparison(df, X, model, logit, skip_reason, loo),
    }
    return model, logit, meta


def main():
    df = pd.read_csv(DATA_PATH)
    if df.empty:
        print(f"{DATA_PATH.name} has no rows: nothing to train on. Run build_training_data.py "
              "after more NO2 hours overlap with the traffic capture.", file=sys.stderr)
        return 1
    model, logit, meta = train(df)
    text = json.dumps(meta, indent=2, allow_nan=False) + "\n"  # fail before writing anything
    joblib.dump(model, MODEL_PATH)
    if logit is None:
        LOGISTIC_PATH.unlink(missing_ok=True)  # an older one would contradict model_meta.json
    else:
        joblib.dump(logit, LOGISTIC_PATH)
    META_PATH.write_text(text, encoding="utf-8", newline="\n")  # LF on Windows too

    def fmt(value):
        return "null" if value is None else f"{value:.3f}"

    metrics, rc = meta["metrics"], meta["risk_comparison"]
    print(f"Rows: {len(df)}  ({meta['time_range']['start']} .. {meta['time_range']['end']})")
    print("Coefficients: " + ", ".join(f"{f} = {c:.6f}" for f, c in meta["coefficients"].items())
          + f", intercept = {meta['intercept']:.3f}")
    print(f"Sign check: {meta['sign_check']}")
    print(f"R2 (in-sample): {fmt(metrics['r2_in_sample'])}   MAE (in-sample): "
          f"{fmt(metrics['mae_in_sample'])} ug/m3   MAE (leave-one-out): "
          f"{fmt(metrics['mae_leave_one_out'])}"
          + ("" if len(df) >= MIN_ROWS_FOR_LOO else f" (needs >= {MIN_ROWS_FOR_LOO} rows)"))
    print(f"Evaluation: {meta['evaluation_note']}")
    print(f"Logistic regression (exceeded = NO2 > {THRESHOLD:g} ug/m3, {rc['n_exceeded']} of "
          f"{len(df)} hours): " + ("fitted" if logit is not None
                                   else f"skipped, {rc['logistic']['skip_reason']}"))
    print(f"Risk comparison: {rc['comparison_note']}")
    saved = [MODEL_PATH.name] + ([LOGISTIC_PATH.name] if logit is not None else [])
    print(f"Saved {', '.join(saved)} and {META_PATH.name} (scikit-learn {sklearn.__version__})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
