# -*- coding: utf-8 -*-
import pandas as pd

from mapping.t05002 import T05002
from mapping.t06001 import T06001


def test_ps_name_id():
    ps = T05002(user_name='任震东', id_card_no='150304197609302532')
    mock_df = pd.DataFrame({
        'result': [b'\x01']
    })
    ps._ps_name_id(df=mock_df)
    assert ps.variables['ps_name_id'] == 0


def test_ps_crime_type():
    ps = T06001(user_name='任震东', id_card_no='150304197609302532')
    ps._ps_crime_type(pd.DataFrame({
        'crime_type': [''],
        'case_period': ['[0,3)']
    }))
    print(ps.variables)

    ps._ps_crime_type(pd.DataFrame({
        'crime_type': ['DRUG_RELATED, ILLEGAL_F']
    }))
    assert ps.variables['ps_involve_drug'] == 1
    assert ps.variables['ps_drug'] == 0

    ps._ps_crime_type(pd.DataFrame({
        'crime_type': ['DRUG_RELATED, DRUG']
    }))
    assert ps.variables['ps_involve_drug'] == 1
    assert ps.variables['ps_drug'] == 1

    ps._ps_crime_type(pd.DataFrame({
        'crime_type': ['ILLEGAL_A'],
        'case_period': ['[0, 12)']
    }))
    assert ps.variables['ps_illeg_crim'] == 1
    assert ps.variables['ps_illegal_record_time'] == 1

    ps._ps_crime_type(pd.DataFrame({
        'crime_type': ['ILLEGAL_A'],
        'case_period': ['[12, 24)']
    }))
    assert ps.variables['ps_illeg_crim'] == 1
    assert ps.variables['ps_illegal_record_time'] == 2

    ps._ps_crime_type(pd.DataFrame({
        'crime_type': ['ILLEGAL_A'],
        'case_period': ['[24, 50)']
    }))
    assert ps.variables['ps_illeg_crim'] == 1
    assert ps.variables['ps_illegal_record_time'] == 3

    ps._ps_crime_type(pd.DataFrame({
        'crime_type': ['ILLEGAL_A'],
        'case_period': ['[60, 61)']
    }))
    assert ps.variables['ps_illeg_crim'] == 1
    assert ps.variables['ps_illegal_record_time'] == 4
