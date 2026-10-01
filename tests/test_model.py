"""model.pkl and predict(): the course checks, clamping, the risk curve and the shipped
model_meta.json."""
import json

import pytest

import predict


def test_model_pkl_loads():
    assert hasattr(predict.load_model(), "predict")


def test_predict_returns_float_no2_in_range_and_risk_between_0_and_1():
    result = predict.predict(3000.0, 8)
    assert isinstance(result["no2_ug_m3_predicted"], float)
    assert 0 <= result["no2_ug_m3_predicted"] <= 200
    assert isinstance(result["no2_exceedance_risk"], float)
    assert 0 <= result["no2_exceedance_risk"] <= 1


class _Constant:
    def __init__(self, value):
        self.value = value

    def predict(self, X):
        return [self.value]


def test_negative_prediction_is_clamped_to_zero(monkeypatch):
    monkeypatch.setattr(predict, "_model", _Constant(-12.0))
    result = predict.predict(100, 3)
    assert result["no2_ug_m3_predicted"] == 0.0
    assert result["no2_exceedance_risk"] == round(predict.exceedance_risk(0.0), 4)


def test_risk_is_one_half_at_the_threshold_and_rises_with_no2():
    assert predict.exceedance_risk(predict.THRESHOLD) == pytest.approx(0.5)
    risks = [predict.exceedance_risk(x) for x in (0, 20, 40, 60, 1e6)]
    assert risks == sorted(risks) and risks[0] > 0 and risks[-1] == 1.0


def test_non_finite_input_raises():
    with pytest.raises(ValueError):
        predict.predict(float("nan"), 8)


def test_shipped_meta_matches_the_shipped_models():
    """model_meta.json's risk_comparison agrees with model.pkl and model_logistic.pkl."""
    meta = json.loads(predict.META_PATH.read_text(encoding="utf-8"))
    rc = meta["risk_comparison"]
    assert len(rc["rows"]) == meta["n_rows"]
    logistic_path = predict.MODEL_PATH.with_name("model_logistic.pkl")
    assert rc["logistic"]["fitted"] == logistic_path.exists()
    assert rc["logistic"]["fitted"] or rc["logistic"]["skip_reason"]
    if rc["threshold_ug_m3"] != predict.THRESHOLD:
        pytest.skip("NO2_THRESHOLD differs from the one the model was trained with")
    for row in rc["rows"]:
        result = predict.predict(row["total_intensity_veh_per_hr"], row["hour_of_day"])
        assert result["no2_ug_m3_predicted"] == pytest.approx(row["no2_ug_m3_predicted"], abs=0.01)
        assert result["no2_exceedance_risk"] == pytest.approx(row["regression_sigmoid_risk"],
                                                              abs=1e-4)
        assert (row["logistic_probability"] is None) != rc["logistic"]["fitted"]
