from mapping.mysql_reader import sql_to_df
from mapping.tranformer import Transformer

class Shortmessage(Transformer):
    """
    短信相关的变量模块
    """
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

    def _info_sms_loan_platform(self):
        info_sms_loan_platform = '''
            SELECT COUNT(*) FROM info_sms_loan_platform  WHERE sms_id 
            IN (SELECT id sms_id FROM info_sms WHERE user_name = %(user_name)s AND id_card_no = %(id_card_no)s AND phone = %(phone)s 
            ORDER BY id DESC LIMIT 1);
        '''
        data1 = sql_to_df(sql=(info_sms_loan_platform), params={"user_name": self.user_name, "id_card_no": self.id_card_no, "phone": self.phone})


    def _hd_reg_cnt(self):
        """
        计算短信核查_注册总次数
        :param user_name:
        :param id_card_no:
        :param phone
        :return 短信核查_注册总次数
        """


        return data1

    def hd_app_cnt(self):
        """
        计算短信核查_申请总次数
        :param user_name:
        :param id_card_no:
        :param phone
        :return 短信核查_申请总次数
        """
        sql_str = '''
            SELECT COUNT(*) FROM info_sms_loan_apply WHERE sms_id 
            IN (SELECT id FROM info_sms WHERE user_name = %(user_name)s AND id_card_no = %(id_card_no)s AND phone = %(phone)s 
            ORDER BY id DESC LIMIT 1);
        '''
        data2 = sql_to_df(sql=(sql_str), params={"user_name": self.user_name, "id_card_no": self.id_card_no, "phone": self.phone})

        return data2

