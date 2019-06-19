import pandas as pd
from mapping.short_message import Shortmessage


def test_hd_reg_cnt():
    ps = Shortmessage(user_name='任震东', id_card_no='150304197609302532', phone='15055111234')

    mock_df = pd.DataFrame({
        'result': ['001','002','003']
    })
    ps._hd_reg_cnt(df=mock_df)
    print(ps.variables)



# def test_ps_crime_type():
#     ps = Shortmessage(user_name='任震东', id_card_no='150304197609302532')
#     ps._ps_crime_type(pd.DataFrame({
#         'crime_type': []
#     }))
#     print(ps.variables)
#
#     ps._ps_crime_type(pd.DataFrame({
#         'crime_type': ['DRUG_RELATED, ILLEGAL_F']
#     }))
#     assert ps.variables['ps_involve_drug'] == 1
#     assert ps.variables['ps_drug'] == 0
#
#     ps._ps_crime_type(pd.DataFrame({
#         'crime_type': ['DRUG_RELATED, DRUG']
#     }))
#     assert ps.variables['ps_involve_drug'] == 1
#     assert ps.variables['ps_drug'] == 1

