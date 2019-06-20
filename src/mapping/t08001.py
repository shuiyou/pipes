from mapping.mysql_reader import sql_to_df
from mapping.tranformer import Transformer


class InfoRiskAntiFraud(Transformer):
    """
    欺诈咨询相关的变量模块
    """

    def __init__(self, user_name, id_card_no, phone) -> None:
        super().__init__()
        self.user_name = user_name
        self.id_card_no = id_card_no
        self.phone = phone
        self.variables = {
            "qh_fraudinfo_isMachdBlMakt": 0,
            "qh_fraudinfo_isMachCraCall": 0,
            "qh_fraudinfo_isMachFraud": 0,
            "qh_fraudinfo_isMachEmpty": 0,
            "qh_fraudinfo_isMachYZmobile": 0,
            "qh_fraudinfo_isMachSmallNo": 0,
            "qh_fraudinfo_isMachSZNo": 0
        }

    def _info_risk_anti_fraud_df(self):
        info_risk_anti_fraud = """
            SELECT user_name, id_card_no,phone,expired_at,match_blacklist,match_crank_call,match_fraud,
            match_empty_number,match_verification_mobile,match_small_no,match_sz_no
            FROM info_risk_anti_fraud 
            WHERE  unix_timestamp(NOW()) < unix_timestamp(expired_at)
            AND user_name = %(user_name)s AND id_card_no = %(id_card_no)s AND phone = %(phone)s
            ORDER BY expired_at DESC LIMIT 1;
        """
        df = sql_to_df(sql=(info_risk_anti_fraud),
                       params={"user_name": self.user_name, "id_card_no": self.id_card_no, "phone": self.phone})
        return df

    def _info_risk_anti_fraud(self, df=None):
        if df is not None:
            if df['match_blacklist'] == True:
                self.variables['qh_fraudinfo_isMachdBlMakt'] = 1
            if df['match_crank_call'] == True:
                self.variables['qh_fraudinfo_isMachCraCall'] = 1
            if df['match_fraud'] == True:
                self.variables['qh_fraudinfo_isMachFraud'] = 1
            if df['match_empty_number'] == True:
                self.variables['qh_fraudinfo_isMachEmpty'] = 1
            if df['match_verification_mobile'] == True:
                self.variables['qh_fraudinfo_isMachYZmobile'] = 1
            if df['match_small_no'] == True:
                self.variables['qh_fraudinfo_isMachSmallNo'] = 1
            if df['match_sz_no'] == True:
                self.variables['qh_fraudinfo_isMachSZNo'] = 1

    def transform(self):
        """
        执行变量转换
        :return:
        """
        self._info_risk_anti_fraud(self._info_risk_anti_fraud_df())

    def variables_result(self):
        """
        返回转换好的结果
        :return: dict对象，包含对应的变量
        """
        return self.variables

