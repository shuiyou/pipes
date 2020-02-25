import pandas as pd
from mapping.tranformer import Transformer
from util.mysql_reader import sql_to_df


class T24001(Transformer):
    """
    企业工商核查,贷后新增
    """
    def __init__(self):
        super().__init__()
        self.variables = {
            'com_bus_shares_frost_laf': 0,  # 工商核查_现在是否有股权冻结信息_贷后新增
            'com_bus_shares_impawn_laf': 0,  # 工商核查_现在是否有股权出质登记信息_贷后新增
            'com_bus_mor_detail_laf': 0,  # 工商核查_现在是否有动产抵押登记信息_贷后新增
            # 'com_bus_liquidation_laf': 0  # 工商核查_是否有清算信息_贷后新增
            'com_bus_illegal_list_laf': 0,  # 工商核查_现在是否有严重违法失信信息_贷后新增
            'com_bus_case_info_laf': 0,  # 工商核查_是否有行政处罚信息_贷后新增
            'com_bus_exception_laf': 0  # 工商核查_是否有经营异常信息_贷后新增
        }
        self.pre_biz_date = None
        self.user_name = None
        self.id_card_no = None

    # 判断股权冻结信息是否有新增
    def _com_bus_shares_frost_laf(self):
        sql = """
            select
                count(*) as cnt
            from 
                info_com_bus_shares_frost a
            left join
                (select id FROM info_com_bus_basic where create_time < NOW()
                and (ent_name=%(user_name)s or credit_code=%(id_card_no)s)
                order by create_time desc limit 1) b 
            on
                a.basic_id=b.id
            where
                a.judicial_froz_state like '%%冻结%%' and a.judicial_froz_state not regexp '解冻|解除|失效'
                and a.froz_from between %(result_date)s and NOW()
        """
        df = sql_to_df(sql=sql,
                       params={'result_date': self.pre_biz_date,
                               'user_name': self.user_name,
                               'id_card_no': self.id_card_no})
        if df.values[0][0] > 0:
            self.variables['com_bus_shares_frost_laf'] = 1
        return

    # 判断股权出质登记信息是否有新增
    def _com_bus_shares_impawn_laf(self):
        sql = """
            select
                count(*) as cnt
            from 
                info_com_bus_shares_impawn a
            left join
                (select id FROM info_com_bus_basic where create_time < NOW()
                and (ent_name=%(user_name)s or credit_code=%(id_card_no)s)
                order by create_time desc limit 1) b 
            on
                a.basic_id=b.id
            where
                a.imp_exe_state like '%%有效%%' 
                and a.imp_equple_date between %(result_date)s and NOW()
        """
        df = sql_to_df(sql=sql,
                       params={'result_date': self.pre_biz_date,
                               'user_name': self.user_name,
                               'id_card_no': self.id_card_no})
        if df.values[0][0] > 0:
            self.variables['com_bus_shares_impawn_laf'] = 1
        return

    # 判断动产抵押登记信息是否有新增
    def _com_bus_mor_detail_laf(self):
        sql = """
            select
                count(*) as cnt
            from 
                info_com_bus_mort_basic a
            left join
                (select id FROM info_com_bus_basic where create_time < NOW()
                and (ent_name=%(user_name)s or credit_code=%(id_card_no)s)
                order by create_time desc limit 1) b 
            on
                a.basic_id=b.id
            where
                a.mort_status like '%%有效%%' 
                and a.reg_date between %(result_date)s and NOW()
        """
        df = sql_to_df(sql=sql,
                       params={'result_date': self.pre_biz_date,
                               'user_name': self.user_name,
                               'id_card_no': self.id_card_no})
        if df.values[0][0] > 0:
            self.variables['com_bus_mor_detail_laf'] = 1
        return

    # 判断严重违法失信信息是否有新增
    def _com_bus_illegal_list_laf(self):
        sql = """
            select
                count(*) as cnt
            from 
                info_com_bus_illegal a
            left join
                (select id FROM info_com_bus_basic where create_time < NOW()
                and (ent_name=%(user_name)s or credit_code=%(id_card_no)s)
                order by create_time desc limit 1) b 
            on
                a.basic_id=b.id
            where
                (a.illegal_rresult_out is null or a.illegal_rresult_out = '')
                and a.illegal_date_in between %(result_date)s and NOW()
        """
        df = sql_to_df(sql=sql,
                       params={'result_date': self.pre_biz_date,
                               'user_name': self.user_name,
                               'id_card_no': self.id_card_no})
        if df.values[0][0] > 0:
            self.variables['com_bus_illegal_list_laf'] = 1
        return

    # 判断行政处罚信息是否有新增
    def _com_bus_case_info_laf(self):
        sql = """
            select
                count(*) as cnt
            from 
                info_com_bus_case a
            left join
                (select id FROM info_com_bus_basic where create_time < NOW()
                and (ent_name=%(user_name)s or credit_code=%(id_card_no)s)
                order by create_time desc limit 1) b 
            on
                a.basic_id=b.id
            where
                 a.pen_deciss_date between %(result_date)s and NOW()
        """
        df = sql_to_df(sql=sql,
                       params={'result_date': self.pre_biz_date,
                               'user_name': self.user_name,
                               'id_card_no': self.id_card_no})
        if df.values[0][0] > 0:
            self.variables['com_bus_case_info_laf'] = 1
        return

    # 判断经营异常信息是否有新增
    def _com_bus_exception_laf(self):
        sql = """
            select
                count(*) as cnt
            from 
                info_com_bus_exception a
            left join
                (select id FROM info_com_bus_basic where create_time < NOW()
                and (ent_name=%(user_name)s or credit_code=%(id_card_no)s)
                order by create_time desc limit 1) b 
            on
                a.basic_id=b.id
            where
                (a.result_out is null or a.result_out = '')
                 and a.date_in between %(result_date)s and NOW()
        """
        df = sql_to_df(sql=sql,
                       params={'result_date': self.pre_biz_date,
                               'user_name': self.user_name,
                               'id_card_no': self.id_card_no})
        if df.values[0][0] > 0:
            self.variables['com_bus_case_info_laf'] = 1
        return

    # 执行变量转换
    def transform(self):
        self.pre_biz_date = self.origin_data.get('preBizDate')

        self.variables["variable_product_code"] = "06001"

        self._com_bus_case_info_laf()
        self._com_bus_exception_laf()
        self._com_bus_illegal_list_laf()
        self._com_bus_mor_detail_laf()
        self._com_bus_shares_frost_laf()
        self._com_bus_shares_impawn_laf()
