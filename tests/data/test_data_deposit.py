from data.data_deposit import deposit
from data.data_union_multi_to_one_deposit import unit_deposit


def test_01001_case():
    _assert_df('01001')


def test_02001_case():
    _assert_df('02001')


def test_05001_case():
    _assert_df('05001')


def test_05002_case():
    _assert_df('05002')


def test_06001_case():
    _assert_df('06001')


def test_08001_case():
    _assert_df('08001')


def test_09001_case():
    _assert_df('09001')


def test_11001_case():
    _assert_df('11001')


def test_13001_case():
    _assert_df('13001')


def test_14001_case():
    _assert_df('14001')


def test_16001_case():
    _assert_df('16001')


def test_16002_case():
    _assert_df('16002')


def test_f0003_case():
    _assert_df('f003')


def _assert_union_multi_to_one_df(code):
    ps = unit_deposit();
    df = ps.run(read_file_name="一级测试用例-" + code + ".xlsx")
    result_df = df[df['是否通过'] == 'false']
    if len(result_df) > 0:
        print("测试错了：" + code)
    assert result_df.shape[0] == 0


def _assert_df(code):
    ps = deposit()
    df = ps.run(read_file_name="一级测试用例-" + code + ".xlsx")
    result_df = df[df['是否通过'] == 'false']
    if len(result_df) > 0:
        print("测试错了：" + code)
    assert result_df.shape[0] == 0


def test_num():
    value = 'social_tel_gray_sco='
    expect_result = value.split('=')[1]
    print(expect_result)
