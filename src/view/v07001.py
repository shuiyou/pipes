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
               )
               ORDER BY id DESC 
                       LIMIT 1
                       ;
        """
        df = sql_to_df(sql=sql,
                       params={"user_name": self.user_name, "id_card_no": self.id_card_no, "phone": self.phone})
        return df

    # 计算字段
    def _info_loan_stats(self, df=None):
        if df is not None and len(df) > 0:
            df['recent_mth'] = df['recent_months'].map({'RECENTLY_1M': '最近1个月', 'RECENTLY_3M': '最近3个月',
                                                        'RECENTLY_6M': '最近6个月', 'RECENTLY_12M': '最近12个月'})
            self.variables['loan_analyst_overdue_time'] = df['recent_mth'][0]
            self.overdue_value = df.fillna(0)['low_balance_fail_count'][0] * df.fillna(0)['success_pay_avg_amount'][0]
            print(self.overdue_value)
            if (self.overdue_value > 0) and (self.overdue_value < 2000):
                self.variables['loan_analyst_overdue_amt_interval'] = '0~0.2万'
            elif (self.overdue_value >= 2000) and (self.overdue_value < 5000):
                self.variables['loan_analyst_overdue_amt_interval'] = '0.2万~0.5万'
            elif (self.overdue_value >= 5000) and (self.overdue_value < 10000):
                self.variables['loan_analyst_overdue_amt_interval'] = '0.5万~1万'
            elif (self.overdue_value >= 10000) and (self.overdue_value < 30000):
                self.variables['loan_analyst_overdue_amt_interval'] = '1万~3万'
            elif (self.overdue_value >= 30000) and (self.overdue_value < 50000):
                self.variables['loan_analyst_overdue_amt_interval'] = '3万~5万'
            elif (self.overdue_value >= 50000) and (self.overdue_value < 100000):
                self.variables['loan_analyst_overdue_amt_interval'] = '5万~10万'
            elif self.overdue_value >= 100000:
                self.variables['loan_analyst_overdue_amt_interval'] = '10万以上'

    def transform(self):
        self._info_loan_stats(self._info_loan_stats_df())
