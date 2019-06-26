import pandas as pd
from mapping.t10001 import T10001


def test__ovdu_sco_1y():
    t10001 = T10001()
    t10001.run('赛达尔', '332205199801021324')
    # mock_df = pd.read_csv('data/info_risk_overdue.csv', parse_dates=True)
    # t10001._subtract_by_year(mock_df)
    # t10001._ovdu_sco_1y(df=mock_df)
    # assert t10001.variables['ovdu_sco_1y'] == 307
    print(t10001.variables)
