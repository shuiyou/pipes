import pandas as pd

from mapping.mysql_reader import sql_to_df
from mapping.tranformer import Transformer, subtract_datetime_col


def dongjie(var):
    if ('冻结' in var) and ('解冻' not in var) and ('解除' not in var) and ('失效' not in var):
        result = True
    else:
        result = False
    return result


def jiedong(var):
    if ('解冻' in var) and ('解除' in var) and ('失效' in var):
        result = True
    else:
        result = False
    return result


class Tf0003(Transformer):
    """
    联企工商
    """

    def __init__(self) -> None:
        super().__init__()
        self.variables = {
            'per_com_case_info': 0,
            'per_com_shares_frost': 0,
            'per_com_shares_frost_his': 0,
            'per_com_shares_impawn': 0,
            'per_com_shares_impawn_his': 0,
            'per_com_mor_detail': 0,
            'per_com_mor_detail_his': 0,
            'per_com_liquidation': 0,
            'per_com_exception': 0,
            'per_com_exception_his': 0,
            'per_com_illegal_list': 0,
            'per_com_illegal_list_his': 0,
            'per_com_saicChanLegal': 0,
            'per_com_saicChanInvestor': 0,
            'per_com_exception_result': 0,
            'per_com_saicChanRunscope': 0,
            'per_com_legper_relent_revoke': 0,
            'per_com_legper_outwardCount1': 0,
            'per_com_industryphycode': None,
            'per_com_endtime': None,
            'per_com_openfrom': None,
            'per_com_esdate': None,
            'per_com_areacode': None,
            'per_com_industrycode': None,
            'per_com_saicChanRegister_5y': None,
            'per_com_province': None,
            'per_com_city': None
        }

    def _info_case_df(self):
        info_per_bus_shareholder = """
            SELECT c.ent_name,d.basic_id
            FROM info_com_bus_basic as c
            INNER JOIN info_com_bus_case as d
            on c.id=d.basic_id
            WHERE c.ent_name in (SELECT b.ent_name
            FROM info_per_bus_basic as a
            LEFT JOIN info_per_bus_shareholder as b
            ON a.id=b.basic_id
            WHERE  unix_timestamp(NOW()) < unix_timestamp(a.expired_at)
            AND a.user_name = %(user_name)s AND a.id_card_no = %(id_card_no)s
            AND b.ent_status in ('在营（开业）','存续（在营、开业、在册）')
            ORDER BY b.funded_ratio DESC,b.reg_cap DESC,b.jhi_date LIMIT 1)
           ;
        """
        info_per_bus_legal = """
            SELECT c.ent_name,d.basic_id
            FROM info_com_bus_basic as c
            INNER JOIN info_com_bus_case as d
            on c.id=d.basic_id
            WHERE c.ent_name in  (SELECT b.ent_name
            FROM info_per_bus_basic as a
            LEFT JOIN info_per_bus_legal as b
            ON a.id=b.basic_id
            WHERE  unix_timestamp(NOW()) < unix_timestamp(a.expired_at)
            AND a.user_name = %(user_name)s AND a.id_card_no = %(id_card_no)s
            AND b.ent_status in ('在营（开业）','存续（在营、开业、在册）')
            ORDER BY b.reg_cap DESC,b.jhi_date LIMIT 1)
           ;
        """
        df1 = sql_to_df(sql=info_per_bus_shareholder,
                        params={"user_name": self.user_name, "id_card_no": self.id_card_no})
        df2 = sql_to_df(sql=info_per_bus_legal,
                        params={"user_name": self.user_name, "id_card_no": self.id_card_no})
        return df1, df2

    def _case_info(self, df1=None, df2=None):
        if df1 is not None and len(df1) > 0:
            self.variables['per_com_case_info'] = 1
        elif df2 is not None and len(df2) > 0:
            self.variables['per_com_case_info'] = 1

    def _info_shares_frost_df(self):
        info_per_bus_shareholder = """
            SELECT c.ent_name,d.basic_id,d.judicial_froz_state
            FROM info_com_bus_basic as c
            INNER JOIN info_com_bus_shares_frost as d
            on c.id=d.basic_id
            WHERE c.ent_name in  (SELECT b.ent_name
            FROM info_per_bus_basic as a
            LEFT JOIN info_per_bus_shareholder as b
            ON a.id=b.basic_id
            WHERE  unix_timestamp(NOW()) < unix_timestamp(a.expired_at)
            AND a.user_name = %(user_name)s AND a.id_card_no = %(id_card_no)s
            AND b.ent_status in ('在营（开业）','存续（在营、开业、在册）')
            ORDER BY b.funded_ratio DESC,b.reg_cap DESC,b.jhi_date LIMIT 1)
           ;
        """
        info_per_bus_legal = """
            SELECT c.ent_name,d.basic_id,d.judicial_froz_state
            FROM info_com_bus_basic as c
            INNER JOIN info_com_bus_shares_frost as d
            on c.id=d.basic_id
            WHERE c.ent_name in  (SELECT b.ent_name
            FROM info_per_bus_basic as a
            LEFT JOIN info_per_bus_legal as b
            ON a.id=b.basic_id
            WHERE  unix_timestamp(NOW()) < unix_timestamp(a.expired_at)
            AND a.user_name = %(user_name)s AND a.id_card_no = %(id_card_no)s
            AND b.ent_status in ('在营（开业）','存续（在营、开业、在册）')
            ORDER BY b.reg_cap DESC,b.jhi_date LIMIT 1)
           ;
        """
        df1 = sql_to_df(sql=info_per_bus_shareholder,
                        params={"user_name": self.user_name, "id_card_no": self.id_card_no})
        df2 = sql_to_df(sql=info_per_bus_legal,
                        params={"user_name": self.user_name, "id_card_no": self.id_card_no})
        return df1, df2

    def _shares_frost(self, df1=None, df2=None):
        if df1 is not None and len(df1) > 0:
            df1['dongjie'] = df1.apply(lambda x: dongjie(x['judicial_froz_state']), axis=1)
            df1['jiedong'] = df1.apply(lambda x: jiedong(x['judicial_froz_state']), axis=1)
            if df1[df1['dongjie']].shape[0] > 0:
                self.variables['per_com_shares_frost'] = 1
            if df1[df1['jiedong']].shape[0] > 0:
                self.variables['per_com_shares_frost_his'] = 1
        elif df2 is not None and len(df2) > 0:
            df2['dongjie'] = df2.apply(lambda x: dongjie(x['judicial_froz_state']), axis=1)
            df2['jiedong'] = df2.apply(lambda x: jiedong(x['judicial_froz_state']), axis=1)
            if df2[df2['dongjie']].shape[0] > 0:
                self.variables['per_com_shares_frost'] = 1
            if df2[df2['jiedong']].shape[0] > 0:
                self.variables['per_com_shares_frost_his'] = 1

    def _info_shares_impawn_df(self):
        info_per_bus_shareholder = """
            SELECT c.ent_name,d.basic_id,d.imp_exe_state
            FROM info_com_bus_basic as c
            INNER JOIN info_com_bus_shares_impawn as d
            on c.id=d.basic_id
            WHERE c.ent_name in  (SELECT b.ent_name
            FROM info_per_bus_basic as a
            LEFT JOIN info_per_bus_shareholder as b
            ON a.id=b.basic_id
            WHERE  unix_timestamp(NOW()) < unix_timestamp(a.expired_at)
            AND a.user_name = %(user_name)s AND a.id_card_no = %(id_card_no)s
            AND b.ent_status in ('在营（开业）','存续（在营、开业、在册）')
            ORDER BY b.funded_ratio DESC,b.reg_cap DESC,b.jhi_date LIMIT 1)
           ;
        """
        info_per_bus_legal = """
            SELECT c.ent_name,d.basic_id,d.imp_exe_state
            FROM info_com_bus_basic as c
            INNER JOIN info_com_bus_shares_impawn as d
            on c.id=d.basic_id
            WHERE c.ent_name in  (SELECT b.ent_name
            FROM info_per_bus_basic as a
            LEFT JOIN info_per_bus_legal as b
            ON a.id=b.basic_id
            WHERE  unix_timestamp(NOW()) < unix_timestamp(a.expired_at)
            AND a.user_name = %(user_name)s AND a.id_card_no = %(id_card_no)s
            AND b.ent_status in ('在营（开业）','存续（在营、开业、在册）')
            ORDER BY b.reg_cap DESC,b.jhi_date LIMIT 1)
           ;
        """
        df1 = sql_to_df(sql=info_per_bus_shareholder,
                        params={"user_name": self.user_name, "id_card_no": self.id_card_no})
        df2 = sql_to_df(sql=info_per_bus_legal,
                        params={"user_name": self.user_name, "id_card_no": self.id_card_no})
        return df1, df2

    def _shares_impawn(self, df1=None, df2=None):
        if df1 is not None and len(df1) > 0:
            df1['youxiao'] = df1.apply(lambda x: True if '有效' in x['imp_exe_state'] else False, axis=1)
            df1['shixiao'] = df1.apply(lambda x: True if '失效' in x['imp_exe_state'] else False, axis=1)
            if df1[df1['youxiao']].shape[0] > 0:
                self.variables['per_com_shares_impawn'] = 1
            if df1[df1['shixiao']].shape[0] > 0:
                self.variables['per_com_shares_impawn_his'] = 1
        elif df2 is not None and len(df2) > 0:
            df2['youxiao'] = df2.apply(lambda x: True if '有效' in x['imp_exe_state'] else False, axis=1)
            df2['shixiao'] = df2.apply(lambda x: True if '失效' in x['imp_exe_state'] else False, axis=1)
            if df2[df2['youxiao']].shape[0] > 0:
                self.variables['per_com_shares_impawn'] = 1
            if df2[df2['shixiao']].shape[0] > 0:
                self.variables['per_com_shares_impawn_his'] = 1

    def _info_mor_detail_df(self):
        info_per_bus_shareholder = """
            SELECT c.ent_name,d.basic_id,d.mort_state
            FROM info_com_bus_basic as c
            INNER JOIN info_com_bus_mort_basic as d
            on c.id=d.basic_id
            WHERE c.ent_name in  (SELECT b.ent_name
            FROM info_per_bus_basic as a
            LEFT JOIN info_per_bus_shareholder as b
            ON a.id=b.basic_id
            WHERE  unix_timestamp(NOW()) < unix_timestamp(a.expired_at)
            AND a.user_name = %(user_name)s AND a.id_card_no = %(id_card_no)s
            AND b.ent_status in ('在营（开业）','存续（在营、开业、在册）')
            ORDER BY b.funded_ratio DESC,b.reg_cap DESC,b.jhi_date LIMIT 1)
           ;
        """
        info_per_bus_legal = """
            SELECT c.ent_name,d.basic_id,d.mort_state
            FROM info_com_bus_basic as c
            INNER JOIN info_com_bus_mort_basic as d
            on c.id=d.basic_id
            WHERE c.ent_name in  (SELECT b.ent_name
            FROM info_per_bus_basic as a
            LEFT JOIN info_per_bus_legal as b
            ON a.id=b.basic_id
            WHERE  unix_timestamp(NOW()) < unix_timestamp(a.expired_at)
            AND a.user_name = %(user_name)s AND a.id_card_no = %(id_card_no)s
            AND b.ent_status in ('在营（开业）','存续（在营、开业、在册）')
            ORDER BY b.reg_cap DESC,b.jhi_date LIMIT 1)
           ;
        """
        df1 = sql_to_df(sql=info_per_bus_shareholder,
                        params={"user_name": self.user_name, "id_card_no": self.id_card_no})
        df2 = sql_to_df(sql=info_per_bus_legal,
                        params={"user_name": self.user_name, "id_card_no": self.id_card_no})
        return df1, df2

    def _mor_detail(self, df1=None, df2=None):
        if df1 is not None and len(df1) > 0:
            df1['youxiao'] = df1.apply(lambda x: True if '有效' in x['mort_state'] else False, axis=1)
            df1['shixiao'] = df1.apply(lambda x: True if '失效' in x['mort_state'] else False, axis=1)
            if df1[df1['youxiao']].shape[0] > 0:
                self.variables['per_com_mor_detail'] = 1
            if df1[df1['shixiao']].shape[0] > 0:
                self.variables['per_com_mor_detail_his'] = 1
        elif df2 is not None and len(df2) > 0:
            df2['youxiao'] = df2.apply(lambda x: True if '有效' in x['mort_state'] else False, axis=1)
            df2['shixiao'] = df2.apply(lambda x: True if '失效' in x['mort_state'] else False, axis=1)
            if df2[df2['youxiao']].shape[0] > 0:
                self.variables['per_com_mor_detail'] = 1
            if df2[df2['shixiao']].shape[0] > 0:
                self.variables['per_com_mor_detail_his'] = 1

    def _info_liquidation_df(self):
        info_per_bus_shareholder = """
            SELECT c.ent_name,d.basic_id
            FROM info_com_bus_basic as c
            INNER JOIN info_com_bus_liquidation as d
            on c.id=d.basic_id
            WHERE c.ent_name in  (SELECT b.ent_name
            FROM info_per_bus_basic as a
            LEFT JOIN info_per_bus_shareholder as b
            ON a.id=b.basic_id
            WHERE  unix_timestamp(NOW()) < unix_timestamp(a.expired_at)
            AND a.user_name = %(user_name)s AND a.id_card_no = %(id_card_no)s
            AND b.ent_status in ('在营（开业）','存续（在营、开业、在册）')
            ORDER BY b.funded_ratio DESC,b.reg_cap DESC,b.jhi_date LIMIT 1)
           ;
        """
        info_per_bus_legal = """
            SELECT c.ent_name,d.basic_id
            FROM info_com_bus_basic as c
            INNER JOIN info_com_bus_liquidation as d
            on c.id=d.basic_id
            WHERE c.ent_name in  (SELECT b.ent_name
            FROM info_per_bus_basic as a
            LEFT JOIN info_per_bus_legal as b
            ON a.id=b.basic_id
            WHERE  unix_timestamp(NOW()) < unix_timestamp(a.expired_at)
            AND a.user_name = %(user_name)s AND a.id_card_no = %(id_card_no)s
            AND b.ent_status in ('在营（开业）','存续（在营、开业、在册）')
            ORDER BY b.reg_cap DESC,b.jhi_date LIMIT 1)
           ;
        """
        df1 = sql_to_df(sql=info_per_bus_shareholder,
                        params={"user_name": self.user_name, "id_card_no": self.id_card_no})
        df2 = sql_to_df(sql=info_per_bus_legal,
                        params={"user_name": self.user_name, "id_card_no": self.id_card_no})
        return df1, df2

    def _liquidation_info(self, df1=None, df2=None):
        if df1 is not None and len(df1) > 0:
            self.variables['per_com_liquidation'] = 1
        elif df2 is not None and len(df2) > 0:
            self.variables['per_com_liquidation'] = 1

    def _info_exception_df(self):
        info_per_bus_shareholder = """
            SELECT c.ent_name,d.basic_id,d.result_out,d.result_in,d.date_out
            FROM info_com_bus_basic as c
            INNER JOIN info_com_bus_exception as d
            on c.id=d.basic_id
            WHERE c.ent_name in  (SELECT b.ent_name
            FROM info_per_bus_basic as a
            LEFT JOIN info_per_bus_shareholder as b
            ON a.id=b.basic_id
            WHERE  unix_timestamp(NOW()) < unix_timestamp(a.expired_at)
            AND a.user_name = %(user_name)s AND a.id_card_no = %(id_card_no)s
            AND b.ent_status in ('在营（开业）','存续（在营、开业、在册）')
            ORDER BY b.funded_ratio DESC,b.reg_cap DESC,b.jhi_date LIMIT 1)
           ;
        """
        info_per_bus_legal = """
            SELECT c.ent_name,d.basic_id,d.result_out,d.result_in,d.date_out
            FROM info_com_bus_basic as c
            INNER JOIN info_com_bus_exception as d
            on c.id=d.basic_id
            WHERE c.ent_name in  (SELECT b.ent_name
            FROM info_per_bus_basic as a
            LEFT JOIN info_per_bus_legal as b
            ON a.id=b.basic_id
            WHERE  unix_timestamp(NOW()) < unix_timestamp(a.expired_at)
            AND a.user_name = %(user_name)s AND a.id_card_no = %(id_card_no)s
            AND b.ent_status in ('在营（开业）','存续（在营、开业、在册）')
            ORDER BY b.reg_cap DESC,b.jhi_date LIMIT 1)
           ;
        """
        df1 = sql_to_df(sql=info_per_bus_shareholder,
                        params={"user_name": self.user_name, "id_card_no": self.id_card_no})
        df2 = sql_to_df(sql=info_per_bus_legal,
                        params={"user_name": self.user_name, "id_card_no": self.id_card_no})
        return df1, df2

    def _exception_info(self, df1=None, df2=None):
        if df1 is not None and len(df1) > 0:
            if df1[df1['result_out'] == None].shape[0] > 0:
                self.variables['per_com_exception'] = 1
            else:
                self.variables['per_com_exception_his'] = 1
            if df1[(df1['date_out'] == None) and (df1['result_in'].str.contains('弄虚作假'))].shape[0] > 0:
                self.variables['per_com_exception_result'] = 3
            elif df1[(df1['date_out'] == None) and (df1['result_in'].str.contains('无法联系'))].shape[0] > 0:
                self.variables['per_com_exception_result'] = 2
            elif df1[(df1['date_out'] == None) and (df1['result_in'].str.contains('无法取得联系'))].shape[0] > 0:
                self.variables['per_com_exception_result'] = 2
            elif df1[(df1['date_out'] == None) and (df1['result_in'].str.contains('年度报告'))].shape[0] > 0:
                self.variables['per_com_exception_result'] = 1
            else:
                self.variables['per_com_exception_his'] = 0
        elif df2 is not None and len(df2) > 0:
            if df2[df2['result_out'] == None].shape[0] > 0:
                self.variables['per_com_exception'] = 1
            else:
                self.variables['per_com_exception_his'] = 1
            if df2[(df2['date_out'] == None) and (df2['result_in'].str.contains('弄虚作假'))].shape[0] > 0:
                self.variables['per_com_exception_result'] = 3
            elif df2[(df2['date_out'] == None) and (df2['result_in'].str.contains('无法联系'))].shape[0] > 0:
                self.variables['per_com_exception_result'] = 2
            elif df2[(df2['date_out'] == None) and (df2['result_in'].str.contains('无法取得联系'))].shape[0] > 0:
                self.variables['per_com_exception_result'] = 2
            elif df2[(df2['date_out'] == None) and (df2['result_in'].str.contains('年度报告'))].shape[0] > 0:
                self.variables['per_com_exception_result'] = 1
            else:
                self.variables['per_com_exception_his'] = 0

    def _info_illegal_list_df(self):
        info_per_bus_shareholder = """
            SELECT c.ent_name,d.basic_id,d.illegal_rresult_out
            FROM info_com_bus_basic as c
            INNER JOIN info_com_bus_illegal as d
            on c.id=d.basic_id
            WHERE c.ent_name in  (SELECT b.ent_name
            FROM info_per_bus_basic as a
            LEFT JOIN info_per_bus_shareholder as b
            ON a.id=b.basic_id
            WHERE  unix_timestamp(NOW()) < unix_timestamp(a.expired_at)
            AND a.user_name = %(user_name)s AND a.id_card_no = %(id_card_no)s
            AND b.ent_status in ('在营（开业）','存续（在营、开业、在册）')
            ORDER BY b.funded_ratio DESC,b.reg_cap DESC,b.jhi_date LIMIT 1)
           ;
        """
        info_per_bus_legal = """
            SELECT c.ent_name,d.basic_id,d.illegal_rresult_out
            FROM info_com_bus_basic as c
            INNER JOIN info_com_bus_illegal as d
            on c.id=d.basic_id
            WHERE c.ent_name in  (SELECT b.ent_name
            FROM info_per_bus_basic as a
            LEFT JOIN info_per_bus_legal as b
            ON a.id=b.basic_id
            WHERE  unix_timestamp(NOW()) < unix_timestamp(a.expired_at)
            AND a.user_name = %(user_name)s AND a.id_card_no = %(id_card_no)s
            AND b.ent_status in ('在营（开业）','存续（在营、开业、在册）')
            ORDER BY b.reg_cap DESC,b.jhi_date LIMIT 1)
           ;
        """
        df1 = sql_to_df(sql=info_per_bus_shareholder,
                        params={"user_name": self.user_name, "id_card_no": self.id_card_no})
        df2 = sql_to_df(sql=info_per_bus_legal,
                        params={"user_name": self.user_name, "id_card_no": self.id_card_no})
        return df1, df2

    def _illegal_list_info(self, df1=None, df2=None):
        if df1 is not None and len(df1) > 0:
            if df1[df1['illegal_rresult_out'] == None].shape[0] > 0:
                self.variables['per_com_illegal_list'] = 1
            else:
                self.variables['per_com_illegal_list_his'] = 1
        elif df2 is not None and len(df2) > 0:
            if df2[df2['illegal_rresult_out'] == None].shape[0] > 0:
                self.variables['per_com_illegal_list'] = 1
            else:
                self.variables['per_com_illegal_list_his'] = 1

    def _info_saicChanLegal_df(self):
        info_per_bus_shareholder = """
            SELECT c.ent_name,c.create_time,d.basic_id,d.alt_date,d.alt_item
            FROM info_com_bus_basic as c
            INNER JOIN info_com_bus_alter as d
            on c.id=d.basic_id
            WHERE c.ent_name in  (SELECT b.ent_name
            FROM info_per_bus_basic as a
            LEFT JOIN info_per_bus_shareholder as b
            ON a.id=b.basic_id
            WHERE  unix_timestamp(NOW()) < unix_timestamp(a.expired_at)
            AND a.user_name = %(user_name)s AND a.id_card_no = %(id_card_no)s
            AND b.ent_status in ('在营（开业）','存续（在营、开业、在册）')
            ORDER BY b.funded_ratio DESC,b.reg_cap DESC,b.jhi_date LIMIT 1)
           ;
        """
        info_per_bus_legal = """
            SELECT c.ent_name,c.create_time,d.basic_id,d.alt_date,d.alt_item
            FROM info_com_bus_basic as c
            INNER JOIN info_com_bus_alter as d
            on c.id=d.basic_id
            WHERE c.ent_name in  (SELECT b.ent_name
            FROM info_per_bus_basic as a
            LEFT JOIN info_per_bus_legal as b
            ON a.id=b.basic_id
            WHERE  unix_timestamp(NOW()) < unix_timestamp(a.expired_at)
            AND a.user_name = %(user_name)s AND a.id_card_no = %(id_card_no)s
            AND b.ent_status in ('在营（开业）','存续（在营、开业、在册）')
            ORDER BY b.reg_cap DESC,b.jhi_date LIMIT 1)
           ;
        """
        df1 = sql_to_df(sql=info_per_bus_shareholder,
                        params={"user_name": self.user_name, "id_card_no": self.id_card_no})
        df2 = sql_to_df(sql=info_per_bus_legal,
                        params={"user_name": self.user_name, "id_card_no": self.id_card_no})
        return df1, df2

    def _saicChanLegal_info(self, df1=None, df2=None):
        if df1 is not None and len(df1) > 0:
            self.year1 = subtract_datetime_col(df1, 'create_time', 'alt_date', 'Y')
            self.variables['per_com_saicChanLegal'] = \
                df1[(df1['alt_item'].str.contains('法定代表人')) & (df1[self.year1] < 5)].shape[0]
            self.variables['per_com_saicChanInvestor'] = \
                df1[(df1['alt_item'].str.contains('投资人')) & (df1[self.year1] < 5)].shape[0]
            self.variables['per_com_saicChanRunscope'] = df1[df1['alt_item'].str.contains('经营范围')].shape[0]
            self.variables['per_com_saicChanRegister_5y'] = \
                df1[(df1['alt_item'].str.contains('注册资本')) & (df1[self.year1] < 5)].shape[0]
        elif df2 is not None and len(df2) > 0:
            self.year2 = subtract_datetime_col(df2, 'create_time', 'alt_date', 'Y')
            self.variables['per_com_saicChanLegal'] = \
                df2[(df2['alt_item'].str.contains('法定代表人')) & (df1[self.year1] < 5)].shape[0]
            self.variables['per_com_saicChanInvestor'] = \
                df2[(df2['alt_item'].str.contains('投资人')) & (df1[self.year1] < 5)].shape[0]
            self.variables['per_com_saicChanRunscope'] = df2[df2['alt_item'].str.contains('经营范围')].shape[0]
            self.variables['per_com_saicChanRegister_5y'] = \
                df2[(df2['alt_item'].str.contains('注册资本')) & (df2[self.year1] < 5)].shape[0]

    def _info_legper_df(self):
        info_per_bus_shareholder_entinvitem = """
            SELECT c.ent_name,c.create_time,d.basic_id,d.funded_ratio,d. ent_status,d.ent_name
            FROM info_com_bus_basic as c
            INNER JOIN info_com_bus_entinvitem as d
            on c.id=d.basic_id
            WHERE c.ent_name in  (SELECT b.ent_name
            FROM info_per_bus_basic as a
            LEFT JOIN info_per_bus_shareholder as b
            ON a.id=b.basic_id
            WHERE  unix_timestamp(NOW()) < unix_timestamp(a.expired_at)
            AND a.user_name = %(user_name)s AND a.id_card_no = %(id_card_no)s
            AND b.ent_status in ('在营（开业）','存续（在营、开业、在册）')
            ORDER BY b.funded_ratio DESC,b.reg_cap DESC,b.jhi_date LIMIT 1)
           ;
        """
        info_per_bus_legal_entinvitem = """
            SELECT c.ent_name,c.create_time,d.basic_id,d.funded_ratio,d. ent_status,d.ent_name
            FROM info_com_bus_basic as c
            INNER JOIN info_com_bus_entinvitem as d
            on c.id=d.basic_id
            WHERE c.ent_name in  (SELECT b.ent_name
            FROM info_per_bus_basic as a
            LEFT JOIN info_per_bus_legal as b
            ON a.id=b.basic_id
            WHERE  unix_timestamp(NOW()) < unix_timestamp(a.expired_at)
            AND a.user_name = %(user_name)s AND a.id_card_no = %(id_card_no)s
            AND b.ent_status in ('在营（开业）','存续（在营、开业、在册）')
            ORDER BY b.reg_cap DESC,b.jhi_date LIMIT 1)
           ;
        """
        info_per_bus_shareholder_frinv = """
            SELECT c.ent_name,c.create_time,d.basic_id,d.funded_ratio,d. ent_status,d.ent_name
            FROM info_com_bus_basic as c
            INNER JOIN info_com_bus_frinv as d
            on c.id=d.basic_id
            WHERE c.ent_name in  (SELECT b.ent_name
            FROM info_per_bus_basic as a
            LEFT JOIN info_per_bus_shareholder as b
            ON a.id=b.basic_id
            WHERE  unix_timestamp(NOW()) < unix_timestamp(a.expired_at)
            AND a.user_name = %(user_name)s AND a.id_card_no = %(id_card_no)s
            AND b.ent_status in ('在营（开业）','存续（在营、开业、在册）')
            ORDER BY b.funded_ratio DESC,b.reg_cap DESC,b.jhi_date LIMIT 1)
           ;
        """
        info_per_bus_legal_frinv = """
            SELECT c.ent_name,c.create_time,d.basic_id,d.funded_ratio,d. ent_status,d.ent_name
            FROM info_com_bus_basic as c
            INNER JOIN info_com_bus_frinv as d
            on c.id=d.basic_id
            WHERE c.ent_name in  (SELECT b.ent_name
            FROM info_per_bus_basic as a
            LEFT JOIN info_per_bus_legal as b
            ON a.id=b.basic_id
            WHERE  unix_timestamp(NOW()) < unix_timestamp(a.expired_at)
            AND a.user_name = %(user_name)s AND a.id_card_no = %(id_card_no)s
            AND b.ent_status in ('在营（开业）','存续（在营、开业、在册）')
            ORDER BY b.reg_cap DESC,b.jhi_date LIMIT 1)
           ;
        """
        df1 = sql_to_df(sql=info_per_bus_shareholder_entinvitem,
                        params={"user_name": self.user_name, "id_card_no": self.id_card_no})
        df2 = sql_to_df(sql=info_per_bus_legal_entinvitem,
                        params={"user_name": self.user_name, "id_card_no": self.id_card_no})
        df3 = sql_to_df(sql=info_per_bus_shareholder_frinv,
                        params={"user_name": self.user_name, "id_card_no": self.id_card_no})
        df4 = sql_to_df(sql=info_per_bus_legal_frinv,
                        params={"user_name": self.user_name, "id_card_no": self.id_card_no})
        return df1, df2, df3, df4

    def _legper_info(self, df1=None, df2=None, df3=None, df4=None):
        if (df1 is not None and len(df1) > 0) or (df3 is not None and len(df3) > 0):
            if df1[(df1['ent_status'].str.contains('吊销')) & (df1['funded_ratio'] >= 0.2)].shape[0] > 0:
                self.variables['per_com_legper_relent_revoke'] = 1
            elif df3[(df3['ent_status'].str.contains('吊销')) & (df3['funded_ratio'] >= 0.2)].shape[0] > 0:
                self.variables['per_com_legper_relent_revoke'] = 1
            df5 = pd.concat([df1[df1['funded_ratio'] >= 0.2], df3[df3['funded_ratio'] >= 0.2]])
            df5.drop_duplicates(subset=['ent_name'], inplace=True)
            self.variables['per_com_legper_outwardCount1'] = df5.shape[0]
        elif (df2 is not None and len(df2) > 0) or (df4 is not None and len(df4) > 0):
            if df2[(df2['ent_status'].str.contains('吊销')) & (df2['funded_ratio'] >= 0.2)].shape[0] > 0:
                self.variables['per_com_legper_relent_revoke'] = 1
            elif df4[(df4['ent_status'].str.contains('吊销')) & (df4['funded_ratio'] >= 0.2)].shape[0] > 0:
                self.variables['per_com_legper_relent_revoke'] = 1
            df6 = pd.concat([df2[df2['funded_ratio'] >= 0.2], df4[df4['funded_ratio'] >= 0.2]])
            df6.drop_duplicates(subset=['ent_name'], inplace=True)
            self.variables['per_com_legper_outwardCount1'] = df6.shape[0]

    def _info_industryphycode_df(self):
        info_per_bus_shareholder = """
            SELECT c.ent_name,c.create_time,d.basic_id,d.industry_phy_code,d.open_to,d.open_from,d.es_date,
            d.area_code,d.industry_code,d.province,d.city,d.expired_at
            FROM info_com_bus_basic as c
            INNER JOIN info_com_bus_face as d
            on c.id=d.basic_id
            WHERE c.ent_name in  (SELECT b.ent_name
            FROM info_per_bus_basic as a
            LEFT JOIN info_per_bus_shareholder as b
            ON a.id=b.basic_id
            WHERE  unix_timestamp(NOW()) < unix_timestamp(a.expired_at)
            AND a.user_name = %(user_name)s AND a.id_card_no = %(id_card_no)s
            AND b.ent_status in ('在营（开业）','存续（在营、开业、在册）')
            ORDER BY b.funded_ratio DESC,b.reg_cap DESC,b.jhi_date LIMIT 1)
            ORDER BY d.expired_at DESC LIMIT 1
           ;
        """
        info_per_bus_legal = """
            SELECT c.ent_name,c.create_time,d.basic_id,d.industry_phy_code,d.open_to,d.open_from,d.es_date,
            d.area_code,d.industry_code,d.province,d.city,d.expired_at
            FROM info_com_bus_basic as c
            INNER JOIN info_com_bus_face as d
            on c.id=d.basic_id
            WHERE c.ent_name in  (SELECT b.ent_name
            FROM info_per_bus_basic as a
            LEFT JOIN info_per_bus_legal as b
            ON a.id=b.basic_id
            WHERE  unix_timestamp(NOW()) < unix_timestamp(a.expired_at)
            AND a.user_name = %(user_name)s AND a.id_card_no = %(id_card_no)s
            AND b.ent_status in ('在营（开业）','存续（在营、开业、在册）')
            ORDER BY b.reg_cap DESC,b.jhi_date LIMIT 1)
            ORDER BY d.expired_at DESC LIMIT 1
           ;
        """
        df1 = sql_to_df(sql=info_per_bus_shareholder,
                        params={"user_name": self.user_name, "id_card_no": self.id_card_no})
        df2 = sql_to_df(sql=info_per_bus_legal,
                        params={"user_name": self.user_name, "id_card_no": self.id_card_no})
        return df1, df2

    def _industryphycode_info(self, df1=None, df2=None):
        if df1 is not None and len(df1) > 0:
            self.variables['per_com_industryphycode'] = df1['industry_phy_code'][0]
            self.variables['per_com_endtime'] = df1['open_to'][0]
            self.variables['per_com_openfrom'] = df1['open_from'][0]
            self.variables['per_com_esdate'] = df1['es_date'][0]
            self.variables['per_com_areacode'] = df1['area_code'][0]
            self.variables['per_com_industrycode'] = df1['industry_code'][0]
            self.variables['per_com_province'] = df1['province'][0]
            self.variables['per_com_city'] = df1['city'][0]
        elif df2 is not None and len(df2) > 0:
            self.variables['per_com_industryphycode'] = df2['industry_phy_code'][0]
            self.variables['per_com_endtime'] = df2['open_to'][0]
            self.variables['per_com_openfrom'] = df2['open_from'][0]
            self.variables['per_com_esdate'] = df2['es_date'][0]
            self.variables['per_com_areacode'] = df2['area_code'][0]
            self.variables['per_com_industrycode'] = df2['industry_code'][0]
            self.variables['per_com_province'] = df2['province'][0]
            self.variables['per_com_city'] = df2['city'][0]

    def transform(self):
        """
        执行变量转换
        :return:
        """
        case_df = self._info_case_df()
        self._case_info(case_df[0], case_df[1])

        shares_fronts_df = self._info_shares_frost_df()
        self._shares_frost(shares_fronts_df[0], shares_fronts_df[1])

        shares_impawn_df = self._info_shares_impawn_df()
        self._shares_impawn(shares_impawn_df[0], shares_impawn_df[1])

        mor_detail_df = self._info_mor_detail_df()
        self._mor_detail(mor_detail_df[0], mor_detail_df[1])

        liquidation_df = self._info_liquidation_df()
        self._liquidation_info(liquidation_df[0], liquidation_df[1])

        exception_df = self._info_exception_df()
        self._exception_info(exception_df[0], exception_df[1])

        illegal_list_df = self._info_illegal_list_df()
        self._illegal_list_info(illegal_list_df[0], illegal_list_df[1])

        saic_chan_legal_df = self._info_saicChanLegal_df()
        self._saicChanLegal_info(saic_chan_legal_df[0], saic_chan_legal_df[1])

        legper_df = self._info_legper_df()
        self._legper_info(legper_df[0], legper_df[1], legper_df[2], legper_df[3])

        industryphycode_df = self._info_industryphycode_df()
        self._industryphycode_info(industryphycode_df[0], industryphycode_df[1])
