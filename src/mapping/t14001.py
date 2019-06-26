from mapping.mysql_reader import sql_to_df
from mapping.tranformer import Transformer


def months(mth_start, mth_end):
    year1 = mth_start.year
    year2 = mth_end.year
    month1 = mth_start.month
    month2 = mth_end.month
    num = (year2 - year1) * 12 + (month2 - month1)
    return num


class T14001(Transformer):
    """
    社交核查的变量模块
    """

    def __init__(self) -> None:
        super().__init__()
        self.variables = {
            'social_name_tel_in_black': 0,
            'social_idc_name_in_black': 0,
            'social_tel_gray_sco': None,
            'social_query_mac_cnt': None,
            'social_dir_in_black_rate': None,
            'social_indir_in_black_rate': None,
            'social_dir_rel_indir_rate': None,
            'social_reg_app_cnt': 0,
            'social_query_else_cnt': 0,
            'social_query_else_cnt_6m': 0,
            'social_query_else_cnt_24m': 0,
        }

    def _info_social_blacklist_df(self):
        info_social_blacklist = """
            SELECT a.user_name, a.id_card_no,a.phone,a.social_id,a.searched_organization,
            a.expired_at,b.blacklist_name_with_phone,b.blacklist_name_with_idcard 
            FROM info_social as a
            left join info_social_blacklist as b on a.social_id=b.social_id
            WHERE  unix_timestamp(NOW()) < unix_timestamp(a.expired_at)
            AND a.user_name = %(user_name)s AND a.id_card_no = %(id_card_no)s AND a.phone = %(phone)s
            ORDER BY a.expired_at DESC LIMIT 1;
        """
        df = sql_to_df(sql=(info_social_blacklist),
                       params={"user_name": self.user_name, "id_card_no": self.id_card_no, "phone": self.phone})
        return df

    def _blacklist(self, df=None):
        if df is not None and len(df) > 0:
            if df['blacklist_name_with_phone'][0]:
                self.variables['social_name_tel_in_black'] = 1
            if df['blacklist_name_with_idcard'][0]:
                self.variables['social_idc_name_in_black'] = 1
            self.variables['social_query_mac_cnt'] = df['searched_organization'].values[0]

    def _info_social_gray_df(self):
        info_social_gray = """
            SELECT a.user_name, a.id_card_no,a.phone,a.social_id,a.expired_at,b.phone_gray_score,
            b.contacts_class_1_blacklist_cnt,b.contacts_class_1_cnt,b.contacts_class_2_blacklist_cnt,
            b.contacts_router_ratio FROM info_social as a
            left join info_social_gray as b on a.social_id=b.social_id
            WHERE  unix_timestamp(NOW()) < unix_timestamp(a.expired_at)
            AND a.user_name = %(user_name)s AND a.id_card_no = %(id_card_no)s AND a.phone = %(phone)s
            ORDER BY a.expired_at DESC LIMIT 1;
        """
        df = sql_to_df(sql=(info_social_gray),
                       params={"user_name": self.user_name, "id_card_no": self.id_card_no, "phone": self.phone})

        return df

    def _social_gray(self, df=None):
        if df is not None and len(df) > 0:
            df['jxl_dir_in_black_rate'] = df.apply(
                lambda x: x['contacts_class_1_blacklist_cnt'] / x['contacts_class_1_cnt'], axis=1)
            df['jxl_indir_in_black_rate'] = df.apply(
                lambda x: x['contacts_class_2_blacklist_cnt'] / x['contacts_class_1_cnt'], axis=1)
            self.variables['social_tel_gray_sco'] = df['phone_gray_score'].values[0]
            if df['contacts_class_1_cnt'][0] > 0:
                self.variables['social_dir_in_black_rate'] = df['jxl_dir_in_black_rate'].values[0]
            if df['contacts_class_1_cnt'][0] > 0:
                self.variables['social_indir_in_black_rate'] = df['jxl_indir_in_black_rate'].values[0]
            self.variables['social_dir_rel_indir_rate'] = df['contacts_router_ratio'].values[0]

    def _info_social_register_df(self):
        info_social_gray = """
            SELECT a.user_name, a.id_card_no,a.phone,a.social_id,a.expired_at,b.register_count
            FROM info_social as a
            left join info_social_user_register as b on a.social_id=b.social_id
            WHERE  unix_timestamp(NOW()) < unix_timestamp(a.expired_at)
            AND a.user_name = %(user_name)s AND a.id_card_no = %(id_card_no)s AND a.phone = %(phone)s
            ORDER BY a.expired_at DESC LIMIT 1;
        """
        df = sql_to_df(sql=(info_social_gray),
                       params={"user_name": self.user_name, "id_card_no": self.id_card_no, "phone": self.phone})
        return df

    def _social_register(self, df=None):
        if df is not None and len(df) > 0:
            self.variables['social_reg_app_cnt'] = df['register_count'].values[0]

    def _info_searched_history_df(self):
        info_searched_history = """
            SELECT a.user_name, a.id_card_no,a.phone,a.social_id,a.expired_at,a.create_time,
            b.org_self,b.searched_date
            FROM info_social as a
            left join info_social_searched_history as b on a.social_id=b.social_id
            WHERE  unix_timestamp(NOW()) < unix_timestamp(a.expired_at)
            AND a.user_name = %(user_name)s AND a.id_card_no = %(id_card_no)s AND a.phone = %(phone)s;
        """
        df = sql_to_df(sql=(info_searched_history),
                       params={"user_name": self.user_name, "id_card_no": self.id_card_no, "phone": self.phone})

        return df

    def _searched_history(self, df=None):
        if df is not None and len(df) > 0:
            df['mth'] = df.apply(lambda x: months(x['searched_date'], x['create_time']), axis=1)
            self.variables['social_query_else_cnt'] = df[df['org_self'] == False].shape[0]
            self.variables['social_query_else_cnt_6m'] = df[(df['org_self'] == False) & (df['mth'] < 6)].shape[0]
            self.variables['social_query_else_cnt_24m'] = df[(df['org_self'] == False) & (df['mth'] < 24)].shape[0]


    def transform(self):
        """
        执行变量转换
        :return:
        """
        self._blacklist(self._info_social_blacklist_df())
        self._social_gray(self._info_social_gray_df())
        self._social_register(self._info_social_register_df())
        self._searched_history(self._info_searched_history_df())
