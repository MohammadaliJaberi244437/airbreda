"""Print the trained model and how accurate it is, in plain text.

Reads model_meta.json and training_data.csv (both written by train_model.py and
build_training_data.py). Changes nothing and needs no network.

Usage: python tools/show_model.py
"""
import csv
import json
from datetime import datetime
from pathlib import Path
from statistics import fmean
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parent.parent
LOCAL = ZoneInfo("Europe/Amsterdam")


def local(iso, fmt="%d %b %H:%M"):
    return datetime.fromisoformat(iso).astimezone(LOCAL).strftime(fmt)


def main():
    meta = json.loads((ROOT / "model_meta.json").read_text(encoding="utf-8"))
    with (ROOT / "training_data.csv").open(encoding="utf-8") as fh:
        no2 = [float(row["no2_ug_m3"]) for row in csv.DictReader(fh)]

    a = meta["intercept"]
    b = meta["coefficients"]["total_intensity_veh_per_hr"]
    c = meta["coefficients"]["hour_of_day"]
    metrics = meta["metrics"]
    n = meta["n_rows"]

    print("MODEL")
    print(f"  type      linear regression (scikit-learn {meta['sklearn_version']})")
    print(f"  trained   {local(meta['trained_at'])} Amsterdam time, on {n} hourly rows")
    print(f"  data      hours ending {local(meta['time_range']['start'])} to {local(meta['time_range']['end'])}")
    def term(value, digits):
        return f"{'-' if value < 0 else '+'} {abs(value):.{digits}f}"

    print(f"  formula   NO2 = {a:.2f} {term(b, 4)} x traffic {term(c, 3)} x hour")
    print(f"            ({b * 1000:+.1f} ug/m3 per 1,000 vehicles per hour)")

    # The simplest possible predictor, scored the same way: always guess the average NO2
    # of the other hours. A useful model has to beat this.
    baseline = fmean(abs(y - fmean(no2[:i] + no2[i + 1:])) for i, y in enumerate(no2)) if len(no2) > 1 else None

    def show(value):
        return "n/a" if value is None else f"{value:.2f}"

    print()
    print("ACCURACY")
    print(f"  R2, on the training hours                 {show(metrics['r2_in_sample'])}   (1 = perfect, 0 = no better than the average)")
    print(f"  average error, on the training hours      {show(metrics['mae_in_sample'])} ug/m3")
    print(f"  average error, on hours it has not seen   {show(metrics['mae_leave_one_out'])} ug/m3   (leave-one-out)")
    print(f"  baseline: always guess the average        {show(baseline)} ug/m3   (average NO2 is {fmean(no2):.1f})")

    rows = meta.get("risk_comparison", {}).get("rows", [])
    if rows:
        print()
        print("LAST 6 HOURS      traffic   measured   predicted   miss")
        for row in rows[-6:]:
            miss = row["no2_ug_m3_predicted"] - row["no2_ug_m3"]
            print(f"  {local(row['timestamp'])}   {row['total_intensity_veh_per_hr']:7.0f}   {row['no2_ug_m3']:8.1f}   "
                  f"{row['no2_ug_m3_predicted']:9.1f}   {miss:+5.1f}")
        exceeded = sum(1 for row in rows if row["exceeded"])
        caught = sum(1 for row in rows if row["exceeded"] and row["regression_sigmoid_risk"] >= 0.5)
        print()
        print(f"  hours above {meta['risk_comparison']['threshold_ug_m3']:g} ug/m3: {exceeded} of {n}; flagged by the risk score: {caught}")


if __name__ == "__main__":
    main()
