import pandas as pd
from mapping.tranformer import Transformer
from util.mysql_reader import sql_to_df


class V24001(Transformer):
    """
    企业工商核查,贷前+贷后详细信息展示
    """

    def __init__(self):
        super().__init__()
        self.variables = {
            'info_com_bus_shares_frost': None,  # 工商核查_现在是否有股权冻结信息_贷后+贷前
            'info_com_bus_shares_impawn': None,  # 工商核查_现在是否有股权出质登记信息_贷后+贷前
            'info_com_bus_mor_detail': None,  # 工商核查_现在是否有动产抵押登记信息_贷后+贷前
            'info_com_bus_liquidation': None,  # 工商核查_是否有清算信息_贷后+贷前
            'info_com_bus_illegal_list': None,  # 工商核查_现在是否有严重违法失信信息_贷后+贷前
            'info_com_bus_case_info': None,  # 工商核查_是否有行政处罚信息_贷后+贷前
            'info_com_bus_exception': None  # 工商核查_是否有经营异常信息_贷后+贷前
        }
        self.pre_biz_date = None
        self.user_name = None
        self.id_card_no = None

    # 工商核查各命中项详细信息展示
    def _hit_com_bus_info_details(self):
        hit_list = {
            'info_com_bus_shares_frost': {'table_name': 'info_com_bus_shares_frost',
                                          'criteria': "where a.judicial_froz_state like '%%冻结%%' and "
                                                      "a.judicial_froz_state not regexp '解冻|解除|失效'",
                                          'type': '工商核查_股权冻结'},
            'info_com_bus_shares_impawn': {'table_name': 'info_com_bus_shares_impawn',
                                           'criteria': "where a.imp_exe_state like '%%有效%%'",
                                           'type': '工商核查_股权出质'},
            'info_com_bus_mor_detail': {'table_name': 'info_com_bus_mort_basic',
                                        'criteria': "where a.mort_status like '%%有效%%'",
                                        'type': '工商核查_动产抵押'},
            'info_com_bus_liquidation': {'table_name': 'info_com_bus_liquidation',
                                         'criteria': "",
                                         'type': '工商核查_清算信息'},
            'info_com_bus_illegal_list': {'table_name': 'info_com_bus_illegal',
                                          'criteria': "where a.illegal_rresult_out is null "
                                                      "or a.illegal_rresult_out=''",
                                          'type': '工商核查_严重违法失信'},
            'info_com_bus_case_info': {'table_name': 'info_com_bus_case',
                                       'criteria': "",
                                       'type': '行政处罚'},
            'info_com_bus_exception': {'table_name': 'info_com_bus_exception',
                                       'criteria': "where a.result_out is null or a.result_out = ''",
                                       'type': '工商核查_经营异常'}
        }
        for var in hit_list.keys():
            sql_before_loan = """
                select
                    a.*
                from
                    %s a""" % hit_list[var]['table_name'] + """ 
                left join 
                    (select id FROM info_com_bus_basic where %(result_date)s between create_time and expired_at
                    and (ent_name=%(user_name)s or credit_code=%(id_card_no)s) 
                    order by create_time desc limit 1) b
                on
                    a.basic_id=b.id """ + hit_list[var]['criteria']
            sql_after_loan = """
                select
                    a.*
                from
                    %s a""" % hit_list[var]['table_name'] + """ 
                left join 
                    (select id FROM info_com_bus_basic where NOW() between create_time and expired_at
                    and (ent_name=%(user_name)s or credit_code=%(id_card_no)s) 
                    order by create_time desc limit 1) b
                on
                    a.basic_id=b.id """ + hit_list[var]['criteria']
            df_before_loan = sql_to_df(sql=sql_before_loan,
                                       params={'result_date': self.pre_biz_date,
                                               'user_name': self.user_name,
                                               'id_card_no': self.id_card_no})
            df_after_loan = sql_to_df(sql=sql_after_loan,
                                      params={'user_name': self.user_name,
                                              'id_card_no': self.id_card_no})
            self.variables[var] = {'before': [], 'after': [], 'type': hit_list[var]['type']}
            if len(df_before_loan) > 0:
                for row in df_before_loan.itertuples():
                    self.variables[var]['before'].append({})
                    for col in df_before_loan.columns:
                        self.variables[var]['before'][-1][col] = getattr(row, col)
            if len(df_after_loan) > 0:
                for row in df_after_loan.itertuples():
                    self.variables[var]['after'].append({})
                    for col in df_after_loan.columns:
                        self.variables[var]['after'][-1][col] = getattr(row, col)
        return

    # 执行变量转换
    def transform(self):
        self.pre_biz_date = self.origin_data.get('preBizDate')

        self.variables["variable_product_code"] = "06001"

        self._hit_com_bus_info_details()
