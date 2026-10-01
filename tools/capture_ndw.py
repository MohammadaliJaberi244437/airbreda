"""Bootstrap NDW capture: runs on the laptop until the EC2 cron job takes over.

Why this exists: Luchtmeetnet NO2 history can be backfilled from its API later,
but the NDW feed is a live snapshot only. Every hour not captured is a training
row we can never get back, so capture starts before the cloud stack exists.

Per snapshot it keeps:
  data/raw/ndw/YYYY-MM-DD/HHMMSS_meetgegevens.xml.gz   raw feed (audit trail, re-parseable)
  data/ndw/YYYY-MM-DD/HH-{site}.csv                    one parsed row per site, appended
The date/hour in the CSV path come from the measurement time in the data, not the clock.

Usage: python capture_ndw.py [--once]
"""
import csv
import gzip
import json
import logging
import re
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

URL = "https://opendata.ndw.nu/snelheden_en_intensiteiten_meetgegevens.xml.gz"
SITES = {
    "hrl": "RWS01_MONIBAS_0271hrl0063ra",  # A27 mainline, direction 1
    "hrr": "RWS01_MONIBAS_0271hrr0063ra",  # A27 mainline, direction 2
    "vwd": "RWS01_MONIBAS_0270vwd0063ra",  # entry slip road
    "vwa": "RWS01_MONIBAS_0270vwa0063ra",  # exit slip road
}
INTERVAL_S = 300  # 12 samples per hour, averaged per hour when building training data
DATA = Path(__file__).resolve().parent.parent / "data"
FIELDS = ["measured_at", "site", "site_id", "intensity_veh_per_hr", "speed_kmh",
          "lane_flows", "lane_speeds", "speed_invalid_count", "captured_at"]

TIME_RE = re.compile(r"<roa:measurementTimeDefault><roa:timeValue>([^<]+)</roa:timeValue>")
TYPE_RE = re.compile(r'xsi:type="roa:(TrafficFlow|TrafficSpeed)"')
VALUE_RE = re.compile(r"<com:(?:vehicleFlowRate|speed)>(-?[\d.]+)</com:")

DATA.mkdir(parents=True, exist_ok=True)
logging.basicConfig(level=logging.INFO, format="%(message)s",
                    handlers=[logging.StreamHandler(), logging.FileHandler(DATA / "capture.log")])


def log(level, **event):
    event["ts"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    logging.log(level, json.dumps(event))


def parse_site(xml, site_id):
    """Return (measured_at, flows, speeds) for one site, or None if absent."""
    start = xml.find(f'id="{site_id}"')
    if start == -1:
        return None
    block = xml[start:xml.find("</roa:siteMeasurements>", start)]
    t = TIME_RE.search(block)
    flows, speeds = [], []
    # Each quantity is parsed on its own so a missing value cannot bleed into the next one.
    for part in block.split('<roa:physicalQuantity index="')[1:]:
        kind, value = TYPE_RE.search(part), VALUE_RE.search(part)
        if not kind:
            continue
        number = float(value.group(1)) if value else None
        (flows if kind.group(1) == "TrafficFlow" else speeds).append(number)
    return (t.group(1) if t else None), flows, speeds


def summarise(flows, speeds):
    valid_flows = [f for f in flows if f is not None and f >= 0]
    valid_speeds = [s for s in speeds if s is not None and s >= 0]  # NDW uses -1 for "no speed"
    intensity = sum(valid_flows) if valid_flows else None
    speed = round(sum(valid_speeds) / len(valid_speeds), 1) if valid_speeds else None
    invalid = sum(1 for s in speeds if s is None or s < 0)
    return intensity, speed, invalid


def append_row(path, row):
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and row["measured_at"] in path.read_text(encoding="utf-8"):
        return False  # feed not refreshed since the last sample
    new = not path.exists()
    with path.open("a", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=FIELDS)
        if new:
            writer.writeheader()
        writer.writerow(row)
    return True


def capture_once():
    captured = datetime.now(timezone.utc)
    raw = urllib.request.urlopen(URL, timeout=60).read()
    raw_path = DATA / "raw" / "ndw" / captured.strftime("%Y-%m-%d") / f"{captured:%H%M%S}_meetgegevens.xml.gz"
    raw_path.parent.mkdir(parents=True, exist_ok=True)
    raw_path.write_bytes(raw)
    xml = gzip.decompress(raw).decode("utf-8")

    for label, site_id in SITES.items():
        parsed = parse_site(xml, site_id)
        if parsed is None or parsed[0] is None:
            log(logging.WARNING, event="site_missing", source="NDW", site=label, location=site_id)
            continue
        measured_at, flows, speeds = parsed
        intensity, speed, invalid = summarise(flows, speeds)
        m = datetime.fromisoformat(measured_at.replace("Z", "+00:00"))
        row = {"measured_at": measured_at, "site": label, "site_id": site_id,
               "intensity_veh_per_hr": intensity, "speed_kmh": speed,
               "lane_flows": ";".join(str(f) for f in flows),
               "lane_speeds": ";".join(str(s) for s in speeds),
               "speed_invalid_count": invalid,
               "captured_at": captured.isoformat(timespec="seconds")}
        written = append_row(DATA / "ndw" / m.strftime("%Y-%m-%d") / f"{m:%H}-{label}.csv", row)
        if invalid:
            log(logging.WARNING, event="DATA_QUALITY_ERROR", source="NDW", location=site_id,
                field="speed", value=-1, lanes_affected=invalid)
        log(logging.INFO, event="capture_success" if written else "capture_duplicate",
            source="NDW", site=label, measured_at=measured_at,
            intensity_veh_per_hr=intensity, speed_kmh=speed)


def main():
    once = "--once" in sys.argv
    while True:
        try:
            capture_once()
        except Exception as exc:  # keep looping: one failed download must not end the capture
            log(logging.ERROR, event="capture_failed", source="NDW", error=repr(exc))
        if once:
            return
        time.sleep(INTERVAL_S - time.time() % INTERVAL_S)  # align to 5-minute marks


if __name__ == "__main__":
    main()
