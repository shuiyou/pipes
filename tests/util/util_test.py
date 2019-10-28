from view.mapper_detail import round_max


def test_round_max():
    max_arr = [1, 2, 3]
    median_arr = [1, 2, 3]
    v = round_max(max_arr, median_arr, 0.3)
    assert v == 4.0


def test_dic():
    a = {}
    info = a.get("sss")
    print(info)
