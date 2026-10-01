"""Join hourly NDW traffic (S3 CSVs) with Luchtmeetnet NO2 (PostgreSQL) into training_data.csv.

Each NO2 hour label gets the mean intensity of every site over the samples measured in
that hour (features.py). Hours are dropped, and reported, when a site is missing, when a
site's samples cover too little of the hour (SiteWindow.covered, the rule serving uses
too), when the NO2 value is flagged, or when there is no NO2 value for them yet.

Usage: python build_training_data.py   (needs DB_* and S3_BUCKET in the environment)
"""
import sys
from datetime import timezone
from pathlib import Path

import pandas as pd

from common import get_conn, get_s3, setup_logging
from features import MIN_SAMPLES, MIN_SPAN, ONE_HOUR, SITE_LABELS, hourly_site_windows, \
    ndw_key_site, parse_ndw_csv, total_intensity, window_hour_of_day
from predict import THRESHOLD

ROOT = Path(__file__).resolve().parent
OUT_CSV = ROOT / "training_data.csv"
PLOT_PATH = ROOT / "docs" / "img" / "no2_vs_intensity.png"
STATION, COMPONENT = "NL10240", "NO2"
COLUMNS = ["timestamp", "no2_ug_m3"] + [f"intensity_{s}" for s in SITE_LABELS] + \
          ["total_intensity_veh_per_hr", "hour_of_day", "n_samples"]
NO2_SQL = """
    SELECT timestamp, value, is_flagged FROM sensor_readings
    WHERE station_id = %s AND component = %s AND value IS NOT NULL
"""


def load_no2(conn):
    """({label: value} for unflagged rows, {labels of flagged rows})."""
    with conn, conn.cursor() as cur:
        cur.execute(NO2_SQL, (STATION, COMPONENT))
        rows = cur.fetchall()
    usable = {ts.astimezone(timezone.utc): value for ts, value, flagged in rows if not flagged}
    flagged = {ts.astimezone(timezone.utc) for ts, _, is_flagged in rows if is_flagged}
    return usable, flagged


def load_ndw_rows(store):
    """(all CSV rows of every ndw/YYYY-MM-DD/HH-site.csv in the bucket, number of files)."""
    keys = [obj["Key"]
            for page in store.client.get_paginator("list_objects_v2").paginate(
                Bucket=store.bucket, Prefix="ndw/")
            for obj in page.get("Contents", []) if ndw_key_site(obj["Key"])]
    rows = []
    for key in keys:
        rows += parse_ndw_csv(store.get(key))
    return rows, len(keys)


def join(no2, flagged, ndw_rows):
    """(training DataFrame, {reason: [(label, detail)]} of dropped traffic hours)."""
    windows = hourly_site_windows(ndw_rows)
    latest_no2 = max(no2.keys() | flagged, default=None)
    records, dropped = [], {}
    for label in sorted(windows):
        sites, detail = windows[label], ""
        thin = [s for s in SITE_LABELS if s in sites and not sites[s].covered]
        if missing := [s for s in SITE_LABELS if s not in sites]:
            reason = f"site(s) missing: {','.join(missing)}"
        elif thin:
            reason = (f"partial traffic coverage (each site needs >= {MIN_SAMPLES} samples "
                      f"spanning >= {MIN_SPAN.seconds // 60} min)")
            detail = ", ".join(f"{s} {sites[s].describe()}" for s in thin)
        elif label in flagged:
            reason = "NO2 value flagged (stale or suspect)"
        elif label not in no2:
            reason = ("NO2 for this hour not published/ingested yet"
                      if latest_no2 is None or label > latest_no2 else "no NO2 value for this hour")
        else:
            records.append({
                "timestamp": label.isoformat(), "no2_ug_m3": no2[label],
                **{f"intensity_{s}": sites[s].mean for s in SITE_LABELS},
                "total_intensity_veh_per_hr":
                    total_intensity({s: w.mean for s, w in sites.items()}),
                "hour_of_day": window_hour_of_day(label),
                "n_samples": sum(w.n for w in sites.values()),
            })
            continue
        dropped.setdefault(reason, []).append((label, detail))
    return pd.DataFrame(records, columns=COLUMNS), dropped


def save_plot(df, path):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    path.parent.mkdir(parents=True, exist_ok=True)
    fig, ax = plt.subplots(figsize=(7, 4.5), dpi=150)
    ax.axhline(THRESHOLD, color="#9a9a9a", linewidth=1, linestyle="--", zorder=1)
    ax.annotate(f"{THRESHOLD:g} $\\mu$g/m$^3$ risk threshold", xy=(0.01, THRESHOLD),
                xycoords=("axes fraction", "data"),
                xytext=(0, 3), textcoords="offset points", color="#6b6b6b", fontsize=8)
    ax.scatter(df["total_intensity_veh_per_hr"], df["no2_ug_m3"], s=48, color="#2a6fdb",
               edgecolor="white", linewidth=1, zorder=3)
    if len(df) <= 12:  # few points: label each with its local hour
        for row in df.itertuples():
            ax.annotate(f"{row.hour_of_day:02d}h local",
                        (row.total_intensity_veh_per_hr, row.no2_ug_m3), xytext=(6, 4),
                        textcoords="offset points", fontsize=8, color="#444444")
    ax.set_xlabel("Total traffic intensity, 4 A27 NDW sites (veh/h, hourly mean)")
    ax.set_ylabel(r"NO$_2$ at NL10240 Breda ($\mu$g/m$^3$, hourly mean)")
    hours = f"{len(df)} hour" + ("" if len(df) == 1 else "s")
    ax.set_title(f"Hourly NO$_2$ vs. traffic intensity (n = {hours})")
    ax.set_ylim(bottom=0, top=max(THRESHOLD * 1.5,
                                  float(df["no2_ug_m3"].max()) * 1.2 if len(df) else 0.0))
    ax.grid(color="#e6e6e6", linewidth=0.6)
    ax.set_axisbelow(True)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    fig.tight_layout()
    fig.savefig(path)
    plt.close(fig)


def main():
    setup_logging()
    conn, store = get_conn(), get_s3()
    if conn is None or store is None:
        print("DB_HOST and S3_BUCKET must be set (load .env first).", file=sys.stderr)
        return 1
    try:
        no2, flagged = load_no2(conn)
    finally:
        conn.close()
    ndw_rows, n_files = load_ndw_rows(store)
    df, dropped = join(no2, flagged, ndw_rows)
    df.to_csv(OUT_CSV, index=False)
    save_plot(df, PLOT_PATH)

    traffic_hours = len(df) + sum(len(v) for v in dropped.values())
    valid_samples = sum(w.n for sites in hourly_site_windows(ndw_rows).values()
                        for w in sites.values())
    print(f"NO2 {STATION}: {len(no2) + len(flagged)} non-null rows in DB, "
          f"{len(flagged)} flagged (excluded from training)")
    print(f"NDW: {n_files} hourly CSV files, {len(ndw_rows)} samples, "
          f"{len(ndw_rows) - valid_samples} skipped as invalid (no intensity or a lane in "
          f"dataError), {traffic_hours} hourly windows")
    print(f"Joined rows: {len(df)}  ->  {OUT_CSV.name}")
    for reason, hours in dropped.items():
        shown = ", ".join(f"{(lb - ONE_HOUR):%Y-%m-%d %H:%M}-{lb:%H:%M}Z" + (f" ({d})" if d else "")
                          for lb, d in hours[:10])
        shown += " ..." if len(hours) > 10 else ""
        print(f"Dropped {len(hours)} hour(s), {reason}: {shown}")
    if len(df):
        print(f"Time range (NO2 hour labels): {df['timestamp'].iloc[0]} .. "
              f"{df['timestamp'].iloc[-1]}")
    print(f"Plot: {PLOT_PATH.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
