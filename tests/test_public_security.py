# -*- coding: utf-8 -*-
import pandas as pd

from mapping.t06001 import T06001


def test_ps_name_id():
    ps = T06001(user_name='任震东', id_card_no='150304197609302532')
    ps._ps_name_id()
    result = ps.variables_result()
    assert result['ps_name_id'] == 0
    mock_df = pd.DataFrame({
        'result': [b'\x01']
    })
    ps._ps_name_id(df=mock_df)
    assert ps.variables['ps_name_id'] == 1


def test_ps_crime_type():
    ps = T06001(user_name='任震东', id_card_no='150304197609302532')
    ps._ps_crime_type(pd.DataFrame({
        'crime_type': []
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
