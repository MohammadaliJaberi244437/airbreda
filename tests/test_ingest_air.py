import pandas as pd
from ingest_air import filter_no2_readings
def test_filter_no2_readings_handles_null_value():
    df = pd.DataFrame({"component": ["NO2", "NO2", "PM10"], "value": [18.4, None, 22.1],
        "timestamp": ["2024-01-15T08:00:00Z", "2024-01-15T09:00:00Z", "2024-01-15T08:00:00Z"]})
    result = filter_no2_readings(df)
    assert len(result) == 2
    assert result["value"].isnull().sum() == 1
