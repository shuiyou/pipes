from mapping.mysql_reader import sql_to_df
from mapping.tranformer import Transformer, subtract_datetime_col


class T10001(Transformer):
    """
    逾期核查
    """

    def __init__(self) -> None:
        super().__init__()
        self.variables = {
            'ovdu_sco_1y': 0
        }

    def _info_risk_overdue_df(self):
        info_certification = """
            SELECT risk_score, data_build_time, create_time FROM info_risk_overdue 
            WHERE unix_timestamp(NOW()) < unix_timestamp(expired_at)
            AND risk_score is not NULL
            AND user_name = %(user_name)s AND id_card_no = %(id_card_no)s;
        """
        df = sql_to_df(sql=info_certification,
                       params={"user_name": self.user_name,
                               "id_card_no": self.id_card_no})
        self._subtract_by_year(df)
        return df

    def _subtract_by_year(self, df):
        self.diff_year = subtract_datetime_col(df, 'create_time', 'data_build_time', 'Y')

    def _ovdu_sco_1y(self, df=None):
        """
        逾期核查_近1年内最大风险得分
        :param user_name:
        :param id_card_no:
        """
        if df is not None and len(df) > 0:
            df = df.query(self.diff_year < 1)
            self.variables['ovdu_sco_1y'] = df['risk_score'].max()

    def transform(self, user_name=None, id_card_no=None, phone=None):
        self._ovdu_sco_1y()
