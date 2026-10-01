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
