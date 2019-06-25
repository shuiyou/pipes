import pandas as pd

from mapping.t10001 import T10001


def test__ovdu_sco_1y():
    t10001 = T10001()
    t10001.input('111', '1111', '111')
    mock_df = pd.read_csv('data/info_risk_overdue.csv', parse_dates=True)
    t10001._subtract_by_year(mock_df)
    t10001._ovdu_sco_1y(df=mock_df)