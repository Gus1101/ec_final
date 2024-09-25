import pytest
from datetime import datetime, timedelta
from data.functions_data import round_datetime_to_nearest_hour

def test_round_down():
    dt = datetime(2023, 9, 24, 14, 15)
    expected = datetime(2023, 9, 24, 14, 0)
    assert round_datetime_to_nearest_hour(dt) == expected