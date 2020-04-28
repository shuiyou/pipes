from product.date_time_util import after_ref_date
from view.mapper_detail import round_max


def test_round_max():
    max_arr = [1, 2, 3]
    median_arr = [1, 2, 3]
    v = round_max(max_arr, median_arr, 0.3)
    assert v == 4.0


def test_a():
    assert after_ref_date(2020, 3, 2018, 12)
    assert after_ref_date(2020, 3, 2015, 12)
    assert after_ref_date(2018, 3, 2018, 12) is False
    assert after_ref_date(2018, 3, 2017, 12)
