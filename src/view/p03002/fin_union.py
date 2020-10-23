# @Time : 2020/10/21 3:31 PM 
# @Author : lixiaobo
# @File : fin_com.py.py 
# @Software: PyCharm


import json

import jsonpath
import pandas as pd

from mapping.grouped_tranformer import GroupedTransformer, invoke_each, invoke_union
from util.mysql_reader import sql_to_df


class FinCom(GroupedTransformer):
    """
    财务风险分析_fin
    """

    def invoke_style(self) -> int:
        return invoke_union

    def group_name(self):
        return "fin"

    def __init__(self) -> None:
        super().__init__()
        self.variables = {
            'fin_mort_cnt': 0,
            'fin_impawn_cnt': 0,
            'fin_alt_cnt': 0,
            'fin_multi_cnt': 0,
            'fin_mort_name': [],
            'fin_mort_to_name': [],
            'fin_mort_reg_no': [],
            'fin_mort_reg_date': [],
            'fin_mort_status': [],
            'fin_mort_reg_org': [],
            'fin_mab_guar_amt': [],
            'fin_mab_guar_type': [],
            'fin_pef_date_range': [],
            'fin_gua_name': [],
            'fin_gua_own': [],
            'fin_gua_des': [],
            'fin_cancle_date': [],
            'fin_impawn_name': [],
            'fin_impawn_role': [],
            'fin_impawn_equity_no': [],
            'fin_impawn_pled_gor': [],
            'fin_impawn_am': [],
            'fin_impawn_org': [],
            'fin_impawn_state': [],
            'fin_impawn_equple_date': [],
            'fin_impawn_pub_date': [],
            'fin_alt_name': [],
            'fin_alt_item': [],
            'fin_alt_date': [],
            'fin_alt_be': [],
            'fin_alt_af': []
        }

    # 解析传入的json串
    def _jsonpath_load(self, json_dic) -> list:
        """
        :param str: json串
        :return list: 返回字典列表，包含类型和相关数据数据
        """
        res = list()
        for i in jsonpath.jsonpath(json_dic, "$..queryData[*]"):
            t = dict()
            t['baseType'] = i.get('userType')
            t['name'] = i.get('name')
            t['idno'] = i.get('idno')
            t['phone'] = i.get('phone')
            res.append(t)
        return res

    # 读取企业数据 info_com_bus_basic 读取企业数据主键
    def _load_info_com_bus_basic_id(self, dict_in) -> int:
        if len(dict_in['idno']) != 0:
            sql = """
               SELECT *
               FROM info_com_bus_basic WHERE ent_name = %(ent_name)s
               and credit_code = %(credit_code)s
               and channel_api_no = 24001
               and unix_timestamp(NOW()) < unix_timestamp(expired_at);
            """
            df = sql_to_df(sql=sql, params={"ent_name": dict_in['name'], "credit_code": dict_in['idno']})
            if df is not None and len(df) > 0:
                df.sort_values(by=['expired_at'], ascending=False, inplace=True)
                return int(df['id'].iloc[0])
        else:
            sql = """
               SELECT *
               FROM info_com_bus_basic WHERE ent_name = %(ent_name)s
               and channel_api_no = 24001
               and unix_timestamp(NOW()) < unix_timestamp(expired_at);
            """
            df = sql_to_df(sql=sql, params={"ent_name": dict_in['name']})
            if df is not None and len(df) > 0:
                df.sort_values(by=['expired_at'], ascending=False, inplace=True)
                return int(df['id'].iloc[0])
        return

    # 读取 info_com_bus_mort_basic 数据
    def _load_info_com_bus_mort_basic_df(self, id_list) -> pd.DataFrame:
        sql = """
               SELECT *
               FROM info_com_bus_mort_basic
               WHERE basic_id in (%(id_list)s)  and jhi_role = '抵押人' and mort_status = '有效';
        """
        info_com_bus_mort_basic_df = sql_to_df(sql=sql, params={"id_list": ",".join(id_list)})
        if info_com_bus_mort_basic_df is not None and len(info_com_bus_mort_basic_df) > 0:
            return info_com_bus_mort_basic_df
        return None

    # 读取 info_com_bus_shares_impawn 数据
    def _load_info_com_bus_shares_impawn_df(self, id_list) -> pd.DataFrame:
        sql = """
               SELECT *
               FROM info_com_bus_shares_impawn
               WHERE basic_id = (%(id_list)s) and imp_exe_state = '有效';
        """
        info_com_bus_shares_impawn_df = sql_to_df(sql=sql, params={"id_list": ",".join(id_list)})
        if info_com_bus_shares_impawn_df is not None and len(info_com_bus_shares_impawn_df) > 0:
            return info_com_bus_shares_impawn_df
        return None

    # 读取 info_com_bus_alter 数据
    def _load_info_com_bus_alter_df(self, id_list) -> pd.DataFrame:
        sql = """
        SELECT * FROM info_com_bus_basic as a JOIN info_com_bus_alter as b on a.id = b.basic_id WHERE a.id in (%(id_list)s);
        """
        info_com_bus_alter_df = sql_to_df(sql=sql, params={"id_list": ",".join(id_list)})
        if info_com_bus_alter_df is not None and len(info_com_bus_alter_df) > 0:
            return info_com_bus_alter_df
        return None

    # 读取 info_com_bus_mort_registe 数据
    def _load_info_com_bus_mort_registe_df(self, id_list) -> pd.DataFrame:
        sql = """
        SELECT * FROM info_com_bus_mort_registe as a LEFT JOIN info_com_bus_mort_basic as b on a.mort_id = b.basic_id where a.mort_id in (%(id_list)s) and b.jhi_role = '抵押人' and b.mort_status = '有效';
        """
        info_com_bus_mort_registe_df = sql_to_df(sql=sql, params={"id_list": ",".join(id_list)})
        if info_com_bus_mort_registe_df is not None and len(info_com_bus_mort_registe_df) > 0:
            return info_com_bus_mort_registe_df
        return None

    # 读取 info_com_bus_mort_collateral 数据
    def _load_info_com_bus_mort_collateral_df(self, id_list) -> pd.DataFrame:
        sql = """
        SELECT * FROM info_com_bus_mort_collateral as a LEFT JOIN info_com_bus_mort_basic as b on a.mort_id = b.basic_id where a.mort_id in (%(id_list)s) and b.jhi_role = '抵押人' and b.mort_status = '有效';
        """
        info_com_bus_mort_registe_df = sql_to_df(sql=sql, params={"id_list": ",".join(id_list)})
        if info_com_bus_mort_registe_df is not None and len(info_com_bus_mort_registe_df) > 0:
            return info_com_bus_mort_registe_df
        return None

    # 读取 info_com_bus_mort_cancel 数据
    def _load_info_com_bus_mort_cancel_df(self, id_list) -> pd.DataFrame:
        sql = """
        SELECT * FROM info_com_bus_mort_cancel as a LEFT JOIN info_com_bus_mort_basic as b on a.mort_id = b.basic_id where a.mort_id in (%(id_list)s) and b.jhi_role = '抵押人' and b.mort_status = '有效';
        """
        info_com_bus_mort_cancel_df = sql_to_df(sql=sql, params={"id_list": ",".join(id_list)})
        if info_com_bus_mort_cancel_df is not None and len(info_com_bus_mort_cancel_df) > 0:
            return info_com_bus_mort_cancel_df
        return None

    # 计算 fin_mort 相关字段
    def _fin_mort(self, df=None):
        if df is not None and len(df) > 0:
            df = df.drop_duplicates().sort_values(by=['mort_gager', 'reg_date'], ascending=False)
            self.variables['fin_mort_cnt'] += len(df)
            self.variables['fin_mort_name'] += df['mort_gager'].to_list()
            self.variables['fin_mort_reg_no'] += df['mort_reg_no'].to_list()
            self.variables['fin_mort_reg_date'] += df['reg_date'].map(lambda x: x.strftime('%Y-%m-%d')).to_list()
            self.variables['fin_mort_status'] += df['mort_status'].to_list()
            self.variables['fin_mort_reg_org'] += df['reg_org'].to_list()

    # 计算 fin_impawn 相关字段
    def _fin_impawn(self, df=None):
        if df is not None and len(df) > 0:
            df = df.drop_duplicates().sort_values(by=['pl_edge_ent', 'imp_pub_date'], ascending=False)
            self.variables['fin_impawn_cnt'] += len(df)
            self.variables['fin_impawn_name'] += df['pl_edge_ent'].to_list()
            self.variables['fin_impawn_role'] += df['jhi_role'].to_list()
            self.variables['fin_impawn_equity_no'] += df['imp_equity_no'].to_list()
            self.variables['fin_impawn_pled_gor'] += df['imp_pled_gor'].to_list()
            self.variables['fin_impawn_am'] += df['imp_am'].to_list()
            self.variables['fin_impawn_org'] += df['imp_org'].to_list()
            self.variables['fin_impawn_state'] += df['imp_exe_state'].to_list()
            self.variables['fin_impawn_equple_date'] += df['imp_equple_date'].map(
                lambda x: x.strftime('%Y-%m-%d')).to_list()
            self.variables['fin_impawn_pub_date'] += df['imp_pub_date'].map(
                lambda x: x.strftime('%Y-%m-%d')).to_list()

    # 计算 fin_alt 相关字段
    def _fin_alt(self, df=None):
        if df is not None and len(df) > 0:
            df = df.drop_duplicates().sort_values(by=['alt_date'], ascending=False)
            target_fileds = ['股东变更',
                             '股权和公证书',
                             '股权转让信息',
                             '负责人变更',
                             '投资人\(股权\)变更',
                             '投资人信息变更',
                             '投资人及出资信息',
                             '投资人变更',
                             '投资人（股权内部转让）备案',
                             '投资人（股权）变更',
                             '投资人（股权）备案',
                             '投资总额变更']
            found = [df['alt_item'].str.contains(x) for x in target_fileds]
            idx = found[0]
            for i in found:
                idx = idx | i
            self.variables['fin_alt_cnt'] += df[idx].shape[0]
            if df[idx].shape[0] > 0:
                self.variables['fin_alt_name'].append(df['ent_name'].drop_duplicates())
                self.variables['fin_alt_item'] += df[idx]['alt_item'].to_list()
                self.variables['fin_alt_date'] += df[idx]['alt_date'].map(
                    lambda x: x.strftime('%Y-%m-%d')).to_list()
                self.variables['fin_alt_be'] += df[idx]['alt_be'].to_list()
                self.variables['fin_alt_af'] += df[idx]['alt_af'].to_list()

    # 计算 info_com_bus_mort_registe 相关字段
    def _fin_mab(self, df=None):
        if df is not None and len(df) > 0:
            df = df.iloc[:, :12].drop_duplicates().sort_values(by=['mort_gage', 'reg_date'], ascending=False)
            self.variables['mab_guar_amt'] += df['mab_guar_amt'].to_list()
            self.variables['mab_guar_type'] += df['mab_guar_type'].to_list()
            self.variables['fin_pef_date_range'] += (df['pef_per_from'].map(lambda x: x.strftime('%Y-%m-%d')) + " - " + \
                                                    df['pef_per_to'].map(lambda x: x.strftime('%Y-%m-%d'))).to_list()

    # 计算 fin_cancle_date 字段
    def _fin_gua(self, df=None):
        if df is not None and len(df) > 0:
            df = df.iloc[:, :12].drop_duplicates().sort_values(by=['mort_gager', 'reg_date'], ascending=False)
            self.variables['fin_gua_name'] += df['gua_name'].to_list()
            self.variables['fin_gua_own'] += df['gua_own'].to_list()
            self.variables['fin_gua_des'] += df['gua_des'].to_list()

    # 计算 fin_cancle_date 相关字段
    def _fin_cancle(self, df=None):
        if df is not None and len(df) > 0:
            df = df.iloc[:, :12].drop_duplicates().sort_values(by=['mort_gager', 'reg_date'], ascending=False)
            self.variables['fin_cancle_date'] += df['can_date'].to_list()

    def transform(self):
        query_list = self._jsonpath_load(self.full_msg)
        com_list = []
        for each in query_list:
            if each['baseType'].upper() == 'COMPANY':
                com_list.append(each)
        com_id_list = []
        for each in com_list:
            id = str(self._load_info_com_bus_basic_id(each))
            com_id_list.append(id)

        df = self._load_info_com_bus_mort_basic_df(com_id_list)
        self._fin_mort(df)

        df = self._load_info_com_bus_shares_impawn_df(com_id_list)
        self._fin_impawn(df)

        df = self._load_info_com_bus_alter_df(com_id_list)
        self._fin_alt(df)

        df = self._load_info_com_bus_mort_registe_df(com_id_list)
        self._fin_mab(df)

        df = self._load_info_com_bus_mort_collateral_df(com_id_list)
        self._fin_gua(df)

        df = self._load_info_com_bus_mort_cancel_df(com_id_list)
        self._fin_cancle(df)

