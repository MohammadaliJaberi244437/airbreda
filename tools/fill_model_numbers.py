"""Write the trained model's numbers into docs/index.md.

First run: replaces the {{KEY}} placeholders with marker-wrapped values
(<!--KEY-->value<!--/KEY-->, invisible when rendered). Later runs update the
value between the markers, so the document can follow every retrain.

Usage: python tools/fill_model_numbers.py
"""
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parent.parent
DOC = ROOT / "docs" / "index.md"
META = ROOT / "model_meta.json"
LOCAL = ZoneInfo("Europe/Amsterdam")


def fmt_hour(iso):
    if not iso:
        return "n/a"
    d = datetime.fromisoformat(iso).astimezone(timezone.utc)
    return f"{d.day} {d:%B %Y %H:%M} UTC"  # no %-d: not portable to Windows


def fmt_coef(v, decimals=4):
    if v is None:
        return "n/a"
    s = f"{v:.{decimals}f}"
    return s if float(s) != 0 or v == 0 else f"{v:.2e}"


def values(meta):
    m = meta.get("metrics", {})
    c = meta.get("coefficients", {})
    r2 = m.get("r2_in_sample")
    loo = m.get("mae_leave_one_out")
    risk = meta.get("risk_comparison", {})
    logistic = risk.get("logistic", {})
    threshold = risk.get("threshold_ug_m3", 40.0)
    if logistic.get("fitted"):
        logistic_note = (
            f"Stretch goal: a logistic classifier on 'the hour exceeded {threshold:g} ug/m3' was also fitted "
            f"({risk.get('n_exceeded')} of {meta['n_rows']} hours exceed). With so few exceedance hours it cannot "
            "place a reliable boundary, so the sigmoid on the regression stays the served score "
            "(comparison in `model_meta.json`).")
    else:
        logistic_note = (
            f"Stretch goal: a logistic classifier on 'the hour exceeded {threshold:g} ug/m3' was not fitted "
            f"({logistic.get('skip_reason', 'not attempted')}), so the sigmoid on the regression is the served score.")
    trained = datetime.fromisoformat(meta["trained_at"]).astimezone(LOCAL)
    return {
        "N_ROWS": str(meta["n_rows"]),
        "TRAIN_RANGE": f"hours ending {fmt_hour(meta['time_range']['start'])} to {fmt_hour(meta['time_range']['end'])}",
        "R2": "undefined" if r2 is None else f"{r2:.2f}",
        "MAE": "n/a" if m.get("mae_in_sample") is None else f"{m['mae_in_sample']:.2f}",
        "COEF_INTENSITY": fmt_coef(c.get("total_intensity_veh_per_hr")),
        "COEF_HOUR": fmt_coef(c.get("hour_of_day"), 3),
        "INTERCEPT": fmt_coef(meta.get("intercept"), 2),
        "TRAINED_AT": f"{trained:%H:%M} on {trained.day} {trained:%B %Y} (Amsterdam time)",
        # train_model.py's plain-English verdict on the traffic coefficient's sign.
        "SIGN_NOTE": (meta.get("sign_check") or "not recorded").rstrip(".") + ".",
        "MAE_LOO": "not available below 10 rows" if loo is None else f"{loo:.2f} ug/m3",
        "EVAL_NOTE": ("Below 10 rows there is no test set, so these numbers say nothing about predictive skill."
                      if loo is None else
                      "Each hour is predicted by a model fitted on the other hours; neighbouring hours are "
                      "correlated, so even this number is optimistic."),
        "LOGISTIC_NOTE": logistic_note,
    }


def main():
    meta = json.loads(META.read_text(encoding="utf-8"))
    text = DOC.read_text(encoding="utf-8")
    for key, value in values(meta).items():
        wrapped = f"<!--{key}-->{value}<!--/{key}-->"
        text, n_first = re.subn(r"\{\{" + key + r"\}\}", wrapped, text)
        text, n_update = re.subn(rf"<!--{key}-->.*?<!--/{key}-->", wrapped, text, flags=re.S)
        print(f"{key:15} {value!r:60} first={n_first} updated={n_update}")
    DOC.write_text(text, encoding="utf-8")


if __name__ == "__main__":
    main()
