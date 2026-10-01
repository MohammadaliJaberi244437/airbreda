"""model.pkl and predict(): the course checks plus clamping and the risk curve."""
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
