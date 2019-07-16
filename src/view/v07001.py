from mapping.mysql_reader import sql_to_df
from mapping.tranformer import Transformer


class V07001(Transformer):
    """
    信贷统计
    """

    def __init__(self) -> None:
        super().__init__()
        self.variables = {
            'loan_analyst_overdue_time': '',
            'loan_analyst_overdue_amt_interval': ''
        }
    # 读取目标数据集
    def _info_loan_stats_df(self):
        sql = """
               SELECT recent_months,low_balance_fail_count,success_pay_avg_amount 
               FROM info_loan_stats_pay  
               WHERE  loan_stats_id 
               IN (
                   SELECT fv.id 
                   FROM (
                       SELECT id 
                       FROM info_loan_stats 
                       WHERE 
                           user_name = %(user_name)s 
                           AND id_card_no = %(id_card_no)s 
                           AND phone = %(phone)s
                           AND unix_timestamp(NOW()) < unix_timestamp(expired_at)
                       ORDER BY id DESC 
                       LIMIT 1
                   ) as fv
               );
        """
        df = sql_to_df(sql=sql,
                       params={"user_name": self.user_name, "id_card_no": self.id_card_no, "phone": self.phone})
        return df

    # 计算字段
    def _info_loan_stats(self, df=None):
        if df is not None and len(df) > 0:
            df['recent_mth'] = df['recent_months'].map({'RECENTLY_1M': '最近1个月', 'RECENTLY_3M': '最近3个月',
                                                        'RECENTLY_6M': '最近6个月', 'RECENTLY_12M': '最近12个月'})
            df['overdue_amt'] = df.apply(lambda x: x['low_balance_fail_count']*x['success_pay_avg_amount'], axis=1)
            if (df['overdue_amt'][0] > 0) and (df['overdue_amt'][0] < 2000):
                self.variables['loan_analyst_overdue_amt_interval'] = '0~0.2万'
            elif (df['overdue_amt'][0] >= 2000) and (df['overdue_amt'][0] < 5000):
                self.variables['loan_analyst_overdue_amt_interval'] = '0.2万~0.5万'
            elif (df['overdue_amt'][0] >= 5000) and (df['overdue_amt'][0] < 10000):
                self.variables['loan_analyst_overdue_amt_interval'] = '0.5万~1万'
            elif (df['overdue_amt'][0] >= 10000) and (df['overdue_amt'][0] < 30000):
                self.variables['loan_analyst_overdue_amt_interval'] = '1万~3万'
            elif (df['overdue_amt'][0] >= 30000) and (df['overdue_amt'][0] < 50000):
                self.variables['loan_analyst_overdue_amt_interval'] = '3万~5万'
            elif (df['overdue_amt'][0] >= 50000) and (df['overdue_amt'][0] < 100000):
                self.variables['loan_analyst_overdue_amt_interval'] = '5万~10万'
            elif df['overdue_amt'][0] >= 100000:
                self.variables['loan_analyst_overdue_amt_interval'] = '10万以上'

    def transform(self):
        self._info_loan_stats(self._info_loan_stats_df())
