"""NO2 prediction from total traffic intensity and local hour, with an exceedance risk."""
import json
import math
import os
import threading
from pathlib import Path

import joblib
import numpy as np

MODEL_PATH = Path(os.getenv("MODEL_PATH", Path(__file__).with_name("model.pkl")))
META_PATH = MODEL_PATH.with_name("model_meta.json")
STEEPNESS = 0.2  # risk goes from 0.12 to 0.88 between threshold - 10 and threshold + 10
# 40 ug/m3 is the EU annual limit value for NO2 (Directive 2008/50/EC) and the WHO 2005
# annual guideline. Applying it to hourly values is a deliberate mismatch: an hour above 40
# is not a legal exceedance, it only marks hours that push the annual mean the wrong way.
# The EU hourly limit (200 ug/m3) is never approached at this station, so it would make the
# risk always ~0. WHO 2021 guidance is stricter still (annual 10, 24-hour 25 ug/m3).
THRESHOLD = float(os.getenv("NO2_THRESHOLD", "40.0"))

_model = None
_meta = None
_lock = threading.Lock()


def load_model():
    """The fitted model, loaded from MODEL_PATH on first use and cached for the process."""
    global _model
    if _model is None:
        with _lock:
            if _model is None:
                _model = joblib.load(MODEL_PATH)
    return _model


def model_meta():
    """Contents of model_meta.json ({} if it is missing or unreadable), cached."""
    global _meta
    if _meta is None:
        try:
            _meta = json.loads(META_PATH.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return {}
    return _meta


def exceedance_risk(no2_ug_m3):
    """sigmoid(STEEPNESS * (no2 - THRESHOLD)), written to never overflow."""
    z = STEEPNESS * (no2_ug_m3 - THRESHOLD)
    if z >= 0:
        return 1.0 / (1.0 + math.exp(-z))
    return math.exp(z) / (1.0 + math.exp(z))


def predict(total_intensity_veh_per_hr, hour_of_day):
    """{"no2_ug_m3_predicted": float >= 0, "no2_exceedance_risk": float in [0, 1]}."""
    features = [float(total_intensity_veh_per_hr), float(hour_of_day)]
    if not all(math.isfinite(f) for f in features):
        raise ValueError(f"non-finite model input {features}")
    raw = float(load_model().predict(np.array([features]))[0])
    if not math.isfinite(raw):
        raise ValueError(f"model returned {raw}")
    no2 = max(0.0, raw)  # a linear model can go below zero; a concentration cannot
    return {"no2_ug_m3_predicted": round(no2, 2),
            "no2_exceedance_risk": round(exceedance_risk(no2), 4)}
