import pandas as pd
from mapping.mysql_reader import sql_to_df
from mapping.tranformer import Transformer, subtract_datetime_col
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)



class T24001(Transformer):

    """
    工商核查相关的变量模块
    """

    def __init__(self) -> None:

        super().__init__()
        self.variables = {
            'com_bus_status': None,  # 工商核查_企业登记状态异常
            'com_bus_endtime': None,  # 工商核查_营业期限至
            'com_bus_relent_revoke': 0,  # 工商核查_关联公司吊销个数
            'com_bus_case_info': 0,  # 工商核查_现在是否有行政处罚信息
            'com_bus_shares_frost': 0,  # 工商核查_现在是否有股权冻结信息
            'com_bus_shares_frost_his': 0,  # 工商核查_曾经是否有股权冻结信息
            'com_bus_shares_impawn': 0,  # 工商核查_现在是否有股权出质登记信息
            'com_bus_shares_impawn_his': 0,  # 工商核查_曾经是否有股权出质登记信息
            'com_bus_mor_detail': 0,  # 工商核查_现在是否有动产抵押登记信息
            'com_bus_mor_detail_his': 0,  # 工商核查_曾经是否有动产抵押登记信息
            'com_bus_liquidation': 0,  # 工商核查_是否有清算信息
            'com_bus_exception': 0,  # 工商核查_现在是否有经营异常信息
            'com_bus_exception_his': 0,  # 工商核查_曾经是否有经营异常信息
            'com_bus_illegal_list': 0,  # 工商核查_现在是否有严重违法失信信息
            'com_bus_illegal_list_his': 0,  # 工商核查_曾经是否有严重违法失信信息
            'com_bus_registered_capital': None,  # 工商核查_注册资本（万元）
            'com_bus_openfrom': None,  # 工商核查_营业期限自
            'com_bus_enttype': None,  # 工商核查_类型
            'com_bus_esdate': None,  # 工商核查_成立日期
            'com_bus_industryphycode': None,  # 工商核查_行业门类代码
            'com_bus_areacode': None,  # 工商核查_住所所在行政区划代码
            'com_bus_industrycode': None,  # 工商核查_国民经济行业代码
            'com_bus_saicChanLegal_5y': 0,  # 工商核查_法定代表人最近5年内变更次数
            'com_bus_saicChanInvestor_5y': 0,  # 工商核查_投资人最近5年内变更次数
            'com_bus_saicChanRegister_5y': 0,  # 工商核查_注册资本最近5年内变更次数
            'com_bus_saicAffiliated': 0,  # 工商核查_企业关联公司个数
            'com_bus_province': None,  # 工商核查_省
            'com_bus_city': None,  # 工商核查_市
            'com_bus_leg_not_shh': None,  # 工商核查_法人非股东
            'com_bus_exception_result': 0,  # 工商核查_经营异常原因
            'com_bus_saicChanRunscope': 0,  # 工商核查_经营范围变更次数
            'com_bus_legper_relent_revoke': 0,  # 工商核查_企业和法人关联公司是否存在吊销
            'com_bus_legper_outwardCount1': 0  # 工商核查_企业、法人对外投资的公司数量
        }

    # 获取目标数据集1
    def _info_com_bus_face(self):
        sql = '''
            SELECT ent_status,open_to 
            FROM info_com_bus_face 
            WHERE basic_id 
            IN (
                SELECT cbb.basic_id 
                FROM (
                    SELECT id basic_id
                    FROM info_com_bus_basic
                    WHERE ent_name = %(user_name)s 
                        AND credit_code = %(id_card_no)s 
                        AND unix_timestamp(NOW()) < unix_timestamp(expired_at)
                    ORDER BY id DESC 
                    LIMIT 1
                ) cbb
            );
        '''
        df = sql_to_df(sql=sql, params={"user_name": self.user_name, "id_card_no": self.id_card_no})
        return df

    # 计算工商核查_企业登记状态异常
    def _com_bus_status(self, df=None):
        if df is not None and len(df) > 0:
            if df['ent_status'].values[0] == '在营(开业)':
                self.variables['com_bus_status'] = 0
            else:
                self.variables['com_bus_status'] = 1

    # 计算工商核查_营业期限至
    def _com_bus_endtime(self, df=None):
        if df is not None and len(df) > 0:
            self.variables['com_bus_endtime'] = str(df['open_to'].values[0])[0:10]

    # 获取目标数据集2
    def _info_com_bus_entinvitem_frinv(self):
        sql1 = '''
            SELECT ent_name 
            FROM info_com_bus_entinvitem
            WHERE funded_ratio >= 0.2 
            AND ent_status LIKE '%吊销%'
            AND basic_id 
            IN (
                SELECT cbb.basic_id 
                FROM (
                    SELECT id basic_id
                    FROM info_com_bus_basic
                    WHERE ent_name = %(user_name)s 
                        AND credit_code = %(id_card_no)s 
                        AND unix_timestamp(NOW()) < unix_timestamp(expired_at)
                    ORDER BY id DESC 
                    LIMIT 1
                ) cbb
            );
        '''
        sql2 = '''
            SELECT ent_name
            FROM info_com_bus_frinv
            WHERE ent_status LIKE '%吊销%'
                AND fr_name 
                IN (
                    SELECT cbb.ent_name 
                    FROM (
                        SELECT ent_name
                        FROM info_com_bus_basic
                        WHERE ent_name = %(user_name)s 
                            AND credit_code = %(id_card_no)s 
                            AND unix_timestamp(NOW()) < unix_timestamp(expired_at)
                        ORDER BY id DESC 
                        LIMIT 1
                    ) cbb
                )
                AND basic_id 
                IN (
                    SELECT cbb.basic_id 
                    FROM (
                        SELECT id basic_id
                        FROM info_com_bus_basic
                        WHERE ent_name = %(user_name)s 
                            AND credit_code = %(id_card_no)s 
                            AND unix_timestamp(NOW()) < unix_timestamp(expired_at)
                        ORDER BY id DESC 
                        LIMIT 1
                    ) cbb
                );
        '''
        df1 = sql_to_df(sql=sql1, params={"user_name": self.user_name, "id_card_no": self.id_card_no})
        df2 = sql_to_df(sql=sql2, params={"user_name": self.user_name, "id_card_no": self.id_card_no})
        df = pd.concat([df1,df2], ignore_index=True)
        return df

    # 计算工商核查_关联公司吊销个数
    def _com_bus_relent_revoke(self, df=None):
        if df is not None and len(df) > 0:
            df = df.drop_diplicates()
            self.variables['com_bus_relent_revoke'] = len(df)

    # 获取目标数据集3
    def _info_com_bus_case(self):
        sql = '''
            SELECT basic_id
            FROM info_com_bus_case
            WHERE basic_id 
            IN (
                SELECT cbb.basic_id 
                FROM (
                    SELECT id basic_id
                    FROM info_com_bus_basic
                    WHERE ent_name = %(user_name)s 
                        AND credit_code = %(id_card_no)s 
                        AND unix_timestamp(NOW()) < unix_timestamp(expired_at)
                    ORDER BY id DESC 
                    LIMIT 1
                ) cbb
            );
        '''
        df = sql_to_df(sql=sql, params={"user_name": self.user_name, "id_card_no": self.id_card_no})
        return df

    # 计算工商核查_现在是否有行政处罚信息
    def _com_bus_case_info(self, df=None):
        if df is not None and len(df) > 0:
            self.variables['com_bus_case_info'] = 1

    # 获取目标数据集4
    def _info_com_bus_shares_frost(self):
        sql = '''
            SELECT basic_id
            FROM info_com_bus_shares_frost
            WHERE judicial_froz_state LIKE '%冻结%'
            AND judicial_froz_state NOT LIKE '%解冻%'
            AND judicial_froz_state NOT LIKE '%失效%'
            AND judicial_froz_state NOT LIKE '%解除%'
            AND basic_id 
            IN (
                SELECT cbb.basic_id 
                FROM (
                    SELECT id basic_id
                    FROM info_com_bus_basic
                    WHERE ent_name = %(user_name)s 
                        AND credit_code = %(id_card_no)s 
                        AND unix_timestamp(NOW()) < unix_timestamp(expired_at)
                    ORDER BY id DESC 
                    LIMIT 1
                ) cbb
            );
        '''
        df = sql_to_df(sql=sql, params={"user_name": self.user_name, "id_card_no": self.id_card_no})
        return df

    # 计算工商核查_现在是否有股权冻结信息
    def _com_bus_shares_frost(self, df=None):
        if df is not None and len(df) > 0:
            self.variables['com_bus_shares_frost'] = 1

    # 获取目标数据集5
    def _info_com_bus_shares_frost2(self):
        sql = '''
            SELECT basic_id
            FROM info_com_bus_shares_frost
            WHERE judicial_froz_state LIKE '%解冻%'
            OR judicial_froz_state LIKE '%失效%'
            OR judicial_froz_state LIKE '%解除%'
            AND basic_id 
            IN (
                SELECT cbb.basic_id 
                FROM (
                    SELECT id basic_id
                    FROM info_com_bus_basic
                    WHERE ent_name = %(user_name)s 
                        AND credit_code = %(id_card_no)s 
                        AND unix_timestamp(NOW()) < unix_timestamp(expired_at)
                    ORDER BY id DESC 
                    LIMIT 1
                ) cbb
            );
        '''
        df = sql_to_df(sql=sql, params={"user_name": self.user_name, "id_card_no": self.id_card_no})
        return df

    # 计算工商核查_曾经是否有股权冻结信息
    def _com_bus_shares_frost_his(self, df=None):
        if df is not None and len(df) > 0:
            self.variables['com_bus_shares_frost_his'] = 1

    # 获取目标数据集6
    def _info_com_bus_shares_impawn(self):
        sql = '''
            SELECT basic_id
            FROM info_com_bus_shares_impawn
            WHERE imp_exe_state LIKE '%有效%'
            AND basic_id 
            IN (
                SELECT cbb.basic_id 
                FROM (
                    SELECT id basic_id
                    FROM info_com_bus_basic
                    WHERE ent_name = %(user_name)s 
                        AND credit_code = %(id_card_no)s 
                        AND unix_timestamp(NOW()) < unix_timestamp(expired_at)
                    ORDER BY id DESC 
                    LIMIT 1
                ) cbb
            );
        '''
        df = sql_to_df(sql=sql, params={"user_name": self.user_name, "id_card_no": self.id_card_no})
        return df

    # 计算工商核查_现在是否有股权出质登记信息
    def _com_bus_shares_impawn(self, df=None):
        if df is not None and len(df) > 0:
            self.variables['com_bus_shares_impawn'] = 1

    # 获取目标数据集7
    def _info_com_bus_shares_impawn2(self):
        sql = '''
            SELECT basic_id
            FROM info_com_bus_shares_impawn
            WHERE imp_exe_state LIKE '%失效%'
            AND basic_id 
            IN (
                SELECT cbb.basic_id 
                FROM (
                    SELECT id basic_id
                    FROM info_com_bus_basic
                    WHERE ent_name = %(user_name)s 
                        AND credit_code = %(id_card_no)s 
                        AND unix_timestamp(NOW()) < unix_timestamp(expired_at)
                    ORDER BY id DESC 
                    LIMIT 1
                ) cbb
            );
        '''
        df = sql_to_df(sql=sql, params={"user_name": self.user_name, "id_card_no": self.id_card_no})
        return df

    # 计算工商核查_曾经是否有股权出质登记信息
    def _com_bus_shares_impawn_his(self, df=None):
        if df is not None and len(df) > 0:
            self.variables['com_bus_shares_impawn_his'] = 1

    # 获取目标数据集8
    def _info_com_bus_mort_basic(self):
        sql = '''
            SELECT basic_id
            FROM info_com_bus_mort_basic
            WHERE mort_state LIKE '%有效%'
            AND basic_id 
            IN (
                SELECT cbb.basic_id 
                FROM (
                    SELECT id basic_id
                    FROM info_com_bus_basic
                    WHERE ent_name = %(user_name)s 
                        AND credit_code = %(id_card_no)s 
                        AND unix_timestamp(NOW()) < unix_timestamp(expired_at)
                    ORDER BY id DESC 
                    LIMIT 1
                ) cbb
            );
        '''
        df = sql_to_df(sql=sql, params={"user_name": self.user_name, "id_card_no": self.id_card_no})
        return df

    # 计算工商核查_现在是否有动产抵押登记信息
    def _com_bus_mor_detail(self, df=None):
        if df is not None and len(df) > 0:
            self.variables['com_bus_mor_detail'] = 1

    # 获取目标数据集9
    def _info_com_bus_mort_basic2(self):
        sql = '''
            SELECT basic_id
            FROM info_com_bus_mort_basic
            WHERE mort_state LIKE '%失效%'
            AND basic_id 
            IN (
                SELECT cbb.basic_id 
                FROM (
                    SELECT id basic_id
                    FROM info_com_bus_basic
                    WHERE ent_name = %(user_name)s 
                        AND credit_code = %(id_card_no)s 
                        AND unix_timestamp(NOW()) < unix_timestamp(expired_at)
                    ORDER BY id DESC 
                    LIMIT 1
                ) cbb
            );
        '''
        df = sql_to_df(sql=sql, params={"user_name": self.user_name, "id_card_no": self.id_card_no})
        return df

    # 计算工商核查_曾经是否有动产抵押登记信息
    def _com_bus_mor_detail_his(self, df=None):
        if df is not None and len(df) > 0:
            self.variables['com_bus_mor_detail_his'] = 1

    # 获取目标数据集10
    def _info_com_bus_liquidation(self):
        sql = '''
            SELECT basic_id
            FROM info_com_bus_liquidation
            WHERE basic_id 
            IN (
                SELECT cbb.basic_id 
                FROM (
                    SELECT id basic_id
                    FROM info_com_bus_basic
                    WHERE ent_name = %(user_name)s 
                        AND credit_code = %(id_card_no)s 
                        AND unix_timestamp(NOW()) < unix_timestamp(expired_at)
                    ORDER BY id DESC 
                    LIMIT 1
                ) cbb
            );
        '''
        df = sql_to_df(sql=sql, params={"user_name": self.user_name, "id_card_no": self.id_card_no})
        return df

    # 计算工商核查_是否有清算信息
    def _com_bus_liquidation(self, df=None):
        if df is not None and len(df) > 0:
            self.variables['com_bus_liquidation'] = 1

    # 获取目标数据集11
    def _info_com_bus_exception(self):
        sql = '''
            SELECT basic_id
            FROM info_com_bus_exception
            WHERE result_out is NULL
            AND basic_id 
            IN (
                SELECT cbb.basic_id 
                FROM (
                    SELECT id basic_id
                    FROM info_com_bus_basic
                    WHERE ent_name = %(user_name)s 
                        AND credit_code = %(id_card_no)s 
                        AND unix_timestamp(NOW()) < unix_timestamp(expired_at)
                    ORDER BY id DESC 
                    LIMIT 1
                ) cbb
            );
        '''
        df = sql_to_df(sql=sql, params={"user_name": self.user_name, "id_card_no": self.id_card_no})
        return df

    # 计算工商核查_现在是否有经营异常信息
    def _com_bus_exception(self, df=None):
        if df is not None and len(df) > 0:
            self.variables['com_bus_exception'] = 1

    # 获取目标数据集12
    def _info_com_bus_exception2(self):
        sql = '''
            SELECT basic_id
            FROM info_com_bus_exception
            WHERE result_out is not NULL
            AND basic_id 
            IN (
                SELECT cbb.basic_id 
                FROM (
                    SELECT id basic_id
                    FROM info_com_bus_basic
                    WHERE ent_name = %(user_name)s 
                        AND credit_code = %(id_card_no)s 
                        AND unix_timestamp(NOW()) < unix_timestamp(expired_at)
                    ORDER BY id DESC 
                    LIMIT 1
                ) cbb
            );
        '''
        df = sql_to_df(sql=sql, params={"user_name": self.user_name, "id_card_no": self.id_card_no})
        return df

    # 计算工商核查_曾经是否有经营异常信息
    def _com_bus_exception_his(self, df=None):
        if df is not None and len(df) > 0:
            self.variables['com_bus_exception_his'] = 1

    # 获取目标数据集13
    def _info_com_bus_illegal(self):
        sql = '''
            SELECT basic_id
            FROM info_com_bus_illegal
            WHERE illegal_result_out is NULL
            AND basic_id 
            IN (
                SELECT cbb.basic_id 
                FROM (
                    SELECT id basic_id
                    FROM info_com_bus_basic
                    WHERE ent_name = %(user_name)s 
                        AND credit_code = %(id_card_no)s 
                        AND unix_timestamp(NOW()) < unix_timestamp(expired_at)
                    ORDER BY id DESC 
                    LIMIT 1
                ) cbb
            );
        '''
        df = sql_to_df(sql=sql, params={"user_name": self.user_name, "id_card_no": self.id_card_no})
        return df

    # 计算工商核查_现在是否有严重违法失信信息
    def _com_bus_illegal_list(self, df=None):
        if df is not None and len(df) > 0:
            self.variables['com_bus_illegal_list'] = 1

    # 获取目标数据集14
    def _info_com_bus_illegal2(self):
        sql = '''
            SELECT basic_id
            FROM info_com_bus_illegal
            WHERE illegal_result_out is not NULL
            AND basic_id 
            IN (
                SELECT cbb.basic_id 
                FROM (
                    SELECT id basic_id
                    FROM info_com_bus_basic
                    WHERE ent_name = %(user_name)s 
                        AND credit_code = %(id_card_no)s 
                        AND unix_timestamp(NOW()) < unix_timestamp(expired_at)
                    ORDER BY id DESC 
                    LIMIT 1
                ) cbb
            );
        '''
        df = sql_to_df(sql=sql, params={"user_name": self.user_name, "id_card_no": self.id_card_no})
        return df

    # 工商核查_曾经是否有严重违法失信信息
    def _com_bus_illegal_list_his(self, df=None):
        if df is not None and len(df) > 0:
            self.variables['com_bus_illegal_list_his'] = 1






