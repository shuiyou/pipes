from mapping.mysql_reader import sql_to_df
from mapping.tranformer import Transformer,subtract_datetime_col


class T10001(Transformer):
    """
    逾期核查
    """

    def __init__(self) -> None:
        super().__init__()
        self.variables = {
            'ovdu_sco_1y': 0,
            'ovdu_sco': 0,
            'ovdu_sco_time': 0
        }

    def _info_risk_overdue_df(self):
        info_certification = """
            SELECT risk_score, data_build_time, create_time FROM info_risk_overdue 
            WHERE unix_timestamp(NOW()) < unix_timestamp(expired_at)
            AND risk_score is not NULL
            AND user_name = %(user_name)s AND id_card_no = %(id_card_no)s;
        """
        df = sql_to_df(sql=info_certification, params={"user_name": self.user_name, "id_card_no": self.id_card_no})
        df['date_dif'] = df[subtract_datetime_col(df, 'create_time', 'data_build_time', 'Y')]
        return df

    def _ovdu_sco_1y(self, df=None):
        if len(df) > 0:
            df1 = df[df['date_dif'] < 1]
            if len(df1) > 0:
                self.variables['ovdu_sco_1y'] = df1['risk_score'].max()

    def _ovdu_sco(self, df=None):
        if len(df) > 0:
            self.variables['ovdu_sco'] = df['risk_score'].max()

    def _ovdu_sco_time(self, df=None):
        if len(df) > 0:
            df1 = list(df.loc[df['risk_score'] == df['risk_score'].max(), 'date_dif'])
            if df1[0] < 1:
                self.variables['ovdu_sco_time'] = 1
            elif df1[0] < 2:
                self.variables['ovdu_sco_time'] = 2
            elif df1[0] < 3:
                self.variables['ovdu_sco_time'] = 3
            else:
                self.variables['ovdu_sco_time'] = 4

    def transform(self):
        info_risk_overdue_df = self._info_risk_overdue_df()
        self._ovdu_sco_1y(info_risk_overdue_df)
        self._ovdu_sco(info_risk_overdue_df)
        self._ovdu_sco_time(info_risk_overdue_df)
