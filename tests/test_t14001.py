from datetime import datetime

import pandas as pd

from mapping.t14001 import T14001


def test_blacklist():
    ps = T14001()
    mock_df = pd.DataFrame({
        'blacklist_name_with_phone': [True],
        'blacklist_name_with_idcard': [True],
        'searched_organization': [5]
    })
    ps._blacklist(df=mock_df)

    assert ps.variables['jxl_name_tel_in_black'] == 1
    assert ps.variables['jxl_idc_name_in_black'] == 1
    assert ps.variables['jxl_query_mac_cnt'] == 5


def test_social_gray():
    ps = T14001()
    mock_df = pd.DataFrame({
        'phone_gray_score': [100],
        'contacts_class_1_cnt': [0],
        'contacts_class_1_blacklist_cnt': [1.5],
        'contacts_class_2_blacklist_cnt': [3.5],
        'contacts_router_ratio': [0.523]
    })
    ps._social_gray(df=mock_df)
    assert ps.variables['jxl_tel_gray_sco'] == 100
    assert ps.variables['jxl_dir_in_black_rate'] == None
    assert ps.variables['jxl_indir_in_black_rate'] == None
    assert ps.variables['jxl_dir_rel_indir_rate'] == 0.523


def test_social_gray():
    ps = T14001()
    mock_df = pd.DataFrame({
        'register_count': [100]
    })
    ps._social_register(df=mock_df)
    assert ps.variables['jxl_reg_app_cnt'] == 100


def test_searched_history():
    ps = T14001()
    a = [False, False, False, True]
    b = [datetime(2018, 11, 21, 15, 18), datetime(2019, 1, 21, 15, 18), datetime(2019, 4, 21, 15, 18),
         datetime(2019, 5, 21, 15, 18)]
    c = [datetime(2019, 6, 21, 15, 18), datetime(2019, 6, 21, 15, 18), datetime(2019, 6, 21, 15, 18),
         datetime(2019, 6, 21, 15, 18)]

    mock_df = pd.DataFrame({'org_self': a, 'searcheed_date': b, 'create_time': c})
    ps._searched_history(df=mock_df)
    assert ps.variables['jxl_query_else_cnt'] == 3
    assert ps.variables['jxl_query_else_cnt_6m'] == 2


