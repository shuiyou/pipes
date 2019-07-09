from data.data_deposit import deposit
from data.data_union_multi_to_one_deposit import unit_deposit
from data.data_union_multi_to_multi_deposit import unit_multi_deposit
from data.data_single_three_structure import single_three_deposit
import json




def test_01001_case():
    _assert_single_df('01001')

def test_02001_case():
    _assert_single_df('02001')

def test_05001_case():
    _assert_single_df('05001')

def test_05002_case():
    _assert_single_df('05002')

def test_06001_case():
    _assert_single_df('06001')

def test_07001_case():
    _assert_single_df('07001')

def test_08001_case():
    _assert_single_df('08001')

def test_09001_case():
    _assert_single_df('09001')

def test_10001_case():
    _assert_single_df('10001')

def test_11001_case():
    _assert_single_df('11001')

def test_12001_case():
    _assert_single_three_structure_df('12001')

def test_13001_case():
    _assert_single_df('13001')

def test_14001_case():
    _assert_single_df('14001')

def test_16001_case():
    _assert_single_df('16001')

def test_16002_case():
    _assert_single_df('16002')

def test_17001_case():
    _assert_single_df('17001')

def test_18001_case():
    _assert_single_df('18001')

def test_f0001_case():
    _assert_union_multi_to_multi_df('f0001')

def test_f002_case():
    _assert_union_multi_to_multi_df('f002')

def test_f0003_case():
    _assert_union_multi_to_one_df('f003')


def _assert_single_three_structure_df(code):
    ps = single_three_deposit();
    _assert_df(ps,code)

def _assert_union_multi_to_multi_df(code):
    ps = unit_multi_deposit();
    _assert_df(ps, code)

def _assert_union_multi_to_one_df(code):
    ps = unit_deposit();
    _assert_df(ps, code)

def _assert_single_df(code):
    ps = deposit()
    _assert_df(ps,code)



def _assert_df(ps,code):
    df = ps.run(read_file_name="一级测试用例-" + code + ".xlsx")
    result_df = df[df['是否通过'] == 'false']
    if len(result_df) > 0:
        print("测试错了：" + code)
    assert result_df.shape[0] == 0








