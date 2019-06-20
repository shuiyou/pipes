from mapping.mysql_reader import sql_to_df
from mapping.tranformer import Transformer


class T08001(Transformer):
    """
    欺诈咨询相关的变量模块
    """

    def __init__(self) -> None:
        super().__init__()
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
        df = sql_to_df(sql=info_risk_anti_fraud,
                       params={"user_name": self.user_name, "id_card_no": self.id_card_no, "phone": self.phone})
        return df

    def _info_risk_anti_fraud(self, df=None):
        if df is not None:
            if df['match_blacklist']:
                self.variables['qh_fraudinfo_isMachdBlMakt'] = 1
            if df['match_crank_call']:
                self.variables['qh_fraudinfo_isMachCraCall'] = 1
            if df['match_fraud']:
                self.variables['qh_fraudinfo_isMachFraud'] = 1
            if df['match_empty_number']:
                self.variables['qh_fraudinfo_isMachEmpty'] = 1
            if df['match_verification_mobile']:
                self.variables['qh_fraudinfo_isMachYZmobile'] = 1
            if df['match_small_no']:
                self.variables['qh_fraudinfo_isMachSmallNo'] = 1
            if df['match_sz_no']:
                self.variables['qh_fraudinfo_isMachSZNo'] = 1

    def transform(self, user_name=None, id_card_no=None, phone=None):
        """
        执行变量转换
        :return:
        """
        self.user_name = user_name
        self.id_card_no = id_card_no
        self.phone = phone
        self._info_risk_anti_fraud(self._info_risk_anti_fraud_df())

