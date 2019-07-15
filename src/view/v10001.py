from mapping.mysql_reader import sql_to_df
from mapping.tranformer import Transformer, subtract_datetime_col


class V10001(Transformer):
    """
    逾期核查展示
    """

    def __init__(self) -> None:
        super().__init__()
        self.variables = {
            'ovdu_overdue_time_amt': None
        }
    # 读取目标数据集
    def _info_risk_overdue_df(self):
        sql = """
            SELECT b.risk_score, b.data_build_time, a.create_time 
            FROM info_risk_overdue_record b
            LEFT JOIN info_risk_overdue a 
            ON b.risk_overdue_id = a.risk_overdue_id
            WHERE a.risk_overdue_id = (
                SELECT risk_overdue_id FROM info_risk_overdue
                WHERE unix_timestamp(NOW())  < unix_timestamp(expired_at)
                    AND user_name = %(user_name)s 
                    AND id_card_no = %(id_card_no)s
                ORDER BY id DESC 
                LIMIT 1
                );
        """
        df = sql_to_df(sql=sql, params={"user_name": self.user_name,"id_card_no": self.id_card_no})
        df['date_dif'] = df[subtract_datetime_col(df, 'create_time', 'data_build_time', 'M')]
        return df

    # 计算“逾期核查_逾期时间_金额”字段
    def _ovdu_overdue_time_amt(self, df=None):
        new_list = list()
        if df is not None and len(df) > 0:
            for i in range(len(df)):
                if df.iloc[i, 4] < 1 and df.iloc[i, 0] not in [10, 20, 30, 40]:
                    new_list.append('1M' + ':' + str(df.iloc[i, 0])[1])
                elif df.iloc[i, 4] < 3 and df.iloc[i, 0] not in [10, 20, 30, 40]:
                    new_list.append('3M' + ':' + str(df.iloc[i, 0])[1])
                elif df.iloc[i, 4] < 6 and df.iloc[i, 0] not in [10, 20, 30, 40]:
                    new_list.append('6M' + ':' + str(df.iloc[i, 0])[1])
                elif df.iloc[i, 4] < 12 and df.iloc[i, 0] not in [10, 20, 30, 40]:
                    new_list.append('12M' + ':' + str(df.iloc[i, 0])[1])
            if len(new_list) > 0:
                self.variables['ovdu_overdue_time_amt'] = new_list

    def transform(self):
        self._ovdu_overdue_time_amt(self._info_risk_overdue_df())

ps1 = V10001()
ps1.run(user_name='叔杨',id_card_no='340121196008298933')
print(ps1.variables)

ps2 = V10001()
ps2.run(user_name='售点',id_card_no='340121196008298945')
print(ps2.variables)
