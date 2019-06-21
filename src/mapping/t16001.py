from mapping.mysql_reader import sql_to_df
from mapping.tranformer import Transformer


class T16001(Transformer):
    """
    法院核查个人
    """

    def __init__(self) -> None:
        super().__init__()
        self.variables = {
            'court_admi_vio': 0,
            'court_judge': 0,
            'court_trial_proc': 0,
            'court_tax_pay': 0,
            'court_owed_owe': 0,
            'court_tax_arrears': 0,
            'court_dishonesty': 0,
            'court_limit_entry': 0,
            'court_high_cons': 0,
            'court_pub_info': 0,
            'court_cri_sus': 0,
            'court_tax_arrears_amt_3y': 0,
            'court_pub_info_amt_3y': 0,
            'court_admi_vio_amt_3y': 0,
            'court_judge_amt_3y': 0,
            'court_docu_status': 0,
            'court_proc_status': 0
        }

    def _info_admi_vio_df(self):
        info_admi_vio = """
            SELECT a.user_name, a.id_card_no,b.court_id
            FROM info_court as a
            left join info_court_administrative_violation as b on a.id=b.court_id
            WHERE  unix_timestamp(NOW()) < unix_timestamp(a.expired_at)
            AND a.user_name = %(user_name)s AND a.id_card_no = %(id_card_no)s
           ;
        """
        df = sql_to_df(sql=(info_admi_vio),
                       params={"user_name": self.user_name, "id_card_no": self.id_card_no})
        return df

    def _admi_vio(self, df=None):
        if df is not None and len(df) > 0:
            self.variables['court_admi_vio'] = df.shape[0]

    def _info_judge_df(self):
        info_judge = """
            SELECT a.user_name, a.id_card_no,b.court_id
            FROM info_court as a
            left join info_court_judicative_pape as b on a.id=b.court_id
            WHERE  unix_timestamp(NOW()) < unix_timestamp(a.expired_at)
            AND a.user_name = %(user_name)s AND a.id_card_no = %(id_card_no)s
           ;
        """
        df = sql_to_df(sql=(info_judge),
                       params={"user_name": self.user_name, "id_card_no": self.id_card_no})
        return df

    def _judge(self, df=None):
        if df is not None and len(df) > 0:
            self.variables['court_judge'] = df.shape[0]

    def _info_trial_proc_df(self):
        info_trial_proc = """
            SELECT a.user_name, a.id_card_no,b.court_id
            FROM info_court as a
            left join info_court_trial_process as b on a.id=b.court_id
            WHERE  unix_timestamp(NOW()) < unix_timestamp(a.expired_at)
            AND a.user_name = %(user_name)s AND a.id_card_no = %(id_card_no)s
           ;
        """
        df = sql_to_df(sql=(info_trial_proc),
                       params={"user_name": self.user_name, "id_card_no": self.id_card_no})
        return df

    def _trial_proc(self, df=None):
        if df is not None and len(df) > 0:
            self.variables['court_trial_proc'] = df.shape[0]

    def _info_tax_pay_df(self):
        info_tax_pay = """
            SELECT a.user_name, a.id_card_no,b.court_id
            FROM info_court as a
            left join info_court_taxable_abnormal_user as b on a.id=b.court_id
            WHERE  unix_timestamp(NOW()) < unix_timestamp(a.expired_at)
            AND a.user_name = %(user_name)s AND a.id_card_no = %(id_card_no)s
           ;
        """
        df = sql_to_df(sql=(info_tax_pay),
                       params={"user_name": self.user_name, "id_card_no": self.id_card_no})
        return df

    def _tax_pay(self, df=None):
        if df is not None and len(df) > 0:
            self.variables['court_tax_pay'] = df.shape[0]

    def _info_owed_owe_df(self):
        info_owed_owe = """
            SELECT a.user_name, a.id_card_no,b.court_id
            FROM info_court as a
            left join info_court_arrearage as b on a.id=b.court_id
            WHERE  unix_timestamp(NOW()) < unix_timestamp(a.expired_at)
            AND a.user_name = %(user_name)s AND a.id_card_no = %(id_card_no)s
           ;
        """
        df = sql_to_df(sql=(info_owed_owe),
                       params={"user_name": self.user_name, "id_card_no": self.id_card_no})
        return df

    def _owed_owe(self, df=None):
        if df is not None and len(df) > 0:
            self.variables['court_owed_owe'] = df.shape[0]

    def _info_tax_arrears_df(self):
        info_tax_arrears = """
            SELECT a.user_name, a.id_card_no,b.court_id
            FROM info_court as a
            left join info_tax_arrears as b on a.id=b.court_id
            WHERE  unix_timestamp(NOW()) < unix_timestamp(a.expired_at)
            AND a.user_name = %(user_name)s AND a.id_card_no = %(id_card_no)s
           ;
        """
        df = sql_to_df(sql=(info_owed_owe),
                       params={"user_name": self.user_name, "id_card_no": self.id_card_no})
        return df

    def _tax_arrears(self, df=None):
        if df is not None and len(df) > 0:
            self.variables['court_tax_arrears'] = df.shape[0]

    def _info_dishonesty_df(self):
        info_dishonesty = """
            SELECT a.user_name, a.id_card_no,b.court_id
            FROM info_court as a
            left join info_court_deadbeat as b on a.id=b.court_id
            WHERE  unix_timestamp(NOW()) < unix_timestamp(a.expired_at)
            AND a.user_name = %(user_name)s AND a.id_card_no = %(id_card_no)s
           ;
        """
        df = sql_to_df(sql=(info_dishonesty),
                       params={"user_name": self.user_name, "id_card_no": self.id_card_no})
        return df

    def _dishonesty(self, df=None):
        if df is not None and len(df) > 0:
            self.variables['court_dishonesty'] = df.shape[0]

    def _info_limit_entry_df(self):
        info_limit_entry = """
            SELECT a.user_name, a.id_card_no,b.court_id
            FROM info_court as a
            left join info_court_limited_entry_exit as b on a.id=b.court_id
            WHERE  unix_timestamp(NOW()) < unix_timestamp(a.expired_at)
            AND a.user_name = %(user_name)s AND a.id_card_no = %(id_card_no)s
           ;
        """
        df = sql_to_df(sql=(info_limit_entry),
                       params={"user_name": self.user_name, "id_card_no": self.id_card_no})
        return df

    def _limit_entry(self, df=None):
        if df is not None and len(df) > 0:
            self.variables['court_limit_entry'] = df.shape[0]

    def _info_high_cons_df(self):
        info_high_cons = """
            SELECT a.user_name, a.id_card_no,b.court_id
            FROM info_court as a
            left join info_court_limit_hignspending as b on a.id=b.court_id
            WHERE  unix_timestamp(NOW()) < unix_timestamp(a.expired_at)
            AND a.user_name = %(user_name)s AND a.id_card_no = %(id_card_no)s
           ;
        """
        df = sql_to_df(sql=(info_high_cons),
                       params={"user_name": self.user_name, "id_card_no": self.id_card_no})
        return df

    def _high_cons(self, df=None):
        if df is not None and len(df) > 0:
            self.variables['court_high_cons'] = df.shape[0]

    def _info_pub_info_df(self):
        info_pub_info = """
            SELECT a.user_name, a.id_card_no,b.court_id
            FROM info_court as a
            left join info_court_excute_public as b on a.id=b.court_id
            WHERE  unix_timestamp(NOW()) < unix_timestamp(a.expired_at)
            AND a.user_name = %(user_name)s AND a.id_card_no = %(id_card_no)s
           ;
        """
        df = sql_to_df(sql=(info_pub_info),
                       params={"user_name": self.user_name, "id_card_no": self.id_card_no})
        return df

    def _pub_info(self, df=None):
        if df is not None and len(df) > 0:
            self.variables['court_pub_info'] = df.shape[0]

    def _info_cri_sus_df(self):
        info_cri_sus = """
            SELECT a.user_name, a.id_card_no,b.court_id
            FROM info_court as a
            left join info_court_criminal_suspect as b on a.id=b.court_id
            WHERE  unix_timestamp(NOW()) < unix_timestamp(a.expired_at)
            AND a.user_name = %(user_name)s AND a.id_card_no = %(id_card_no)s
           ;
        """
        df = sql_to_df(sql=(info_cri_sus),
                       params={"user_name": self.user_name, "id_card_no": self.id_card_no})
        return df

    def _cri_sus(self, df=None):
        if df is not None and len(df) > 0:
            self.variables['court_cri_sus'] = df.shape[0]

    def transform(self):
        """
        执行变量转换
        :return:
        """
        self._blacklist(self._info_social_blacklist_df())
        self._social_gray(self._info_social_gray_df())
        self._social_register(self._info_social_register_df())
        self._searched_history(self._info_searched_history_df())
