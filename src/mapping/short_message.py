from mapping.mysql_reader import sql_to_df
from mapping.tranformer import Transformer


## 短信核查相关的变量模块
class Shortmessage(Transformer):

    def get_biz_type(self):
        """
        返回这个转换对应的biz type
        :return:
        """
        return ''

    def __init__(self, user_name, id_card_no, phone) -> None:

        super().__init__()
        self.user_name = user_name
        self.id_card_no = id_card_no
        self.phone = phone
        self.variables = {
            'hd_reg_cnt': 0,  # 短信核查_注册总次数
            'hd_reg_cnt_bank_3m': 0,  # 短信核查_近3个月内银行类注册次数
            'hd_reg_cnt_other_3m': 0,  # 短信核查_近3个月内非银行类注册次数
            'hd_app_cnt': 0,  # 短信核查_申请总次数
            'hd_max_apply': 0,  # 短信核查_申请金额最大等级
            'hd_loan_cnt': 0,  # 短信核查_放款总次数
            'hd_max_loan': 0, # 短信核查_放款金额最大等级
            'hd_reject_cnt': 0, # 短信核查_驳回总次数
            'hd_overdue_cnt': 0, # 短信核查_逾期总次数
            'hd_max_overdue': 0,  # 短信核查_逾期金额最大等级
            'hd_owe_cnt': 0,  # 短信核查_欠款总次数
            'hd_max_owe': 0,  # 短信核查_欠款金额最大等级
            'hd_owe_cnt_6m': 0,  # 短信核查_近6个月内欠款次数
            'hd_owe_cnt_6_12m': 0,  # 短信核查_近6-12个月内欠款次数
            'hd_max_owe_6m': 0,  # 短信核查_近6个月内欠款金额最大等级
        }

    ## 获取目标数据集1
    def _info_sms_loan_platform(self):

        sql1 = '''
            SELECT * FROM info_sms_loan_platform  WHERE sms_id 
            IN (SELECT id sms_id FROM info_sms WHERE user_name = %(user_name)s AND id_card_no = %(id_card_no)s AND phone = %(phone)s 
            ORDER BY id DESC LIMIT 1);
        '''
        sql2 = '''
            SELECT id sms_id,create_time FROM info_sms WHERE user_name = %(user_name)s AND id_card_no = %(id_card_no)s AND phone = %(phone)s 
        '''
        df1 = sql_to_df(sql=(sql1), params={"user_name": self.user_name, "id_card_no": self.id_card_no, "phone": self.phone})
        df2 = sql_to_df(sql=(sql2), params={"user_name": self.user_name, "id_card_no": self.id_card_no, "phone": self.phone})
        df = pd.merge(df1,df2,how='left',on='sms_id')
        df['date_dif'] = (df['create_time'] - df['register_time']).map(lambda x: x.days / 30)
        return df

    ## 计算短信核查_注册总次数
    def _hd_reg_cnt(self, df=None):

        self.variables['hd_reg_cnt'] = len(df)

    ## 计算短信核查_近3个月内银行类注册次数
    def _hd_reg_cnt_bank_3m(self, df=None):

        df_1 = df.loc[df['date_dif'] < 3 and df['platform_type'] == 'BANK',:].copy()
        self.variables['hd_reg_cnt_bank_3m'] = len(df_1)

    ## 计算短信核查_近3个月内非银行类注册次数
    def _hd_reg_cnt_other_3m(self, df=None):

        df_1 = df.loc[df['date_dif'] < 3 and df['platform_type'] == 'NON_BANK',:].copy()
        self.variables['hd_reg_cnt_other_3m'] = len(df_1)

    ## 获取目标数据集2
    def _info_sms_loan_apply(self):

        sql = '''
            SELECT * FROM info_sms_loan_apply WHERE sms_id 
            IN (SELECT id sms_id FROM info_sms WHERE user_name = %(user_name)s AND id_card_no = %(id_card_no)s AND phone = %(phone)s 
            ORDER BY id DESC LIMIT 1);
        '''
        df = sql_to_df(sql=(sql),params={"user_name": self.user_name, "id_card_no": self.id_card_no, "phone": self.phone})
        return df

    ## 计算短信核查_申请总次数
    def _hd_app_cnt(self, df=None):

        self.variables['hd_app_cnt'] = len(df)

    ## 计算短信核查_申请金额最大等级
    def _hd_max_apply(self, df=None):

        df['apply_amount'] = df['apply_amount'].replace(to_replace="0W～0.2W", value=1)
        df['apply_amount'] = df['apply_amount'].replace(to_replace="0.2W～0.5W", value=2)
        df['apply_amount'] = df['apply_amount'].replace(to_replace="0.5W～1W", value=3)
        df['apply_amount'] = df['apply_amount'].replace(to_replace="1W～3W", value=4)
        df['apply_amount'] = df['apply_amount'].replace(to_replace="3W～5W", value=5)
        df['apply_amount'] = df['apply_amount'].replace(to_replace="5W～10W", value=6)
        df['apply_amount'] = df['apply_amount'].replace(to_replace="10W以上", value=7)
        self.variables['hd_max_apply'] = df['apply_amount'].max()

    ## 获取目标数据集3
    def _info_sms_loan(self):

        sql = '''
            SELECT * FROM info_sms_loan WHERE sms_id 
            IN (SELECT id sms_id FROM info_sms WHERE user_name = %(user_name)s AND id_card_no = %(id_card_no)s AND phone = %(phone)s 
            ORDER BY id DESC LIMIT 1);
        '''
        df = sql_to_df(sql=(sql),params={"user_name": self.user_name, "id_card_no": self.id_card_no, "phone": self.phone})
        return df

    ## 计算短信核查_放款总次数
    def _hd_loan_cnt(self, df=None):

        self.variables['hd_loan_cnt'] = len(df)

    ## 计算短信核查_放款金额最大等级
    def _hd_max_loan(self, df=None):

        df['loan_amount'] = df['loan_amount'].replace(to_replace="0W～0.2W", value=1)
        df['loan_amount'] = df['loan_amount'].replace(to_replace="0.2W～0.5W", value=2)
        df['loan_amount'] = df['loan_amount'].replace(to_replace="0.5W～1W", value=3)
        df['loan_amount'] = df['loan_amount'].replace(to_replace="1W～3W", value=4)
        df['loan_amount'] = df['loan_amount'].replace(to_replace="3W～5W", value=5)
        df['loan_amount'] = df['loan_amount'].replace(to_replace="5W～10W", value=6)
        df['loan_amount'] = df['loan_amount'].replace(to_replace="10W以上", value=7)
        self.variables['hd_max_loan'] = df['loan_amount'].max()



