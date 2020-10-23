# @Time : 2020/10/21 3:31 PM 
# @Author : lixiaobo
# @File : owner.py.py 
# @Software: PyCharm


import json

import jsonpath
import numpy as np
import pandas as pd

from mapping.grouped_tranformer import GroupedTransformer, invoke_each
from util.DataFrameFlatter import DataFrameFlatter
from util.mysql_reader import sql_to_df


class Owner(GroupedTransformer):
    """
    企业主分析_owner
    """

    def invoke_style(self) -> int:
        return invoke_each

    def group_name(self):
        return "owner"

    def __init__(self) -> None:
        super().__init__()
        self.variables = {
            'owner_info_cnt': 0,
            'owner_tax_cnt': 0,
            'owner_list_cnt': 0,
            'owner_app_cnt': 0,
            'owner_list_name': [],
            'owner_list_style': [],
            'owner_list_detail': [],
            'owner_job_year': 0,
            'owner_major_job_year': 0,
            'owner_tax_name': [],
            'owner_tax_amt': [],
            'owner_tax_type': [],
            'owner_tax_date': [],
            'owner_app_traffic': 0,
            'owner_app_hotel': 0,
            'owner_app_finance': 0,
            'owner_app_invest': 0,
            'owner_app_ent': 0,
            'owner_app_live': 0,
            'owner_app_read': 0,
            'owner_app_game': 0,
            'owner_app_life': 0,
            'owner_app_social': 0,
            'owner_app_edu': 0,
            'owner_app_shop': 0,
            'owner_app_work': 0,
            'owner_app_loan': 0,
            'owner_app_loan_car': 0,
            'owner_app_loan_installment': 0,
            'owner_app_loan_credit': 0,
            'owner_app_loan_cash': 0,
            'owner_app_loan_house': 0,
            'owner_app_loan_p2p': 0,
            'owner_app_loan_platform': 0
        }

    def _jsonpath_load(self, json_str) -> list:
        """
        :param str: json串
        :return list: 返回字典列表，包含类型和相关数据数据
        """
        json_dic = json.loads(json_str)
        res = list()
        for i in jsonpath.jsonpath(json_dic, "$.queryData[*]"):
            t = dict()
            t['baseType'] = i.get('userType')
            t['name'] = i.get('name')
            t['idno'] = i.get('idno')
            t['phone'] = i.get('phone')
            res.append(t)
        return res

    # 读取 info_court 读取risk_subject_id主键
    def _load_info_com_bus_basic_id(self, dict_in) -> int:
        if len(dict_in['idno']) != 0:
            sql = """
               SELECT *
               FROM info_court WHERE unique_name = %(unique_name)s
               and unique_id_no = %(unique_id_no)s
               and unix_timestamp(NOW()) < unix_timestamp(expired_at);
            """
            df = sql_to_df(sql=sql, params={"unique_name": dict_in['name'], "unique_id_no": dict_in['idno']})
            if df is not None and len(df) > 0:
                df.sort_values(by=['expired_at'], ascending=False, inplace=True)
                return int(df['risk_subject_id'].iloc[0])
        else:
            sql = """
               SELECT *
               FROM info_court WHERE unique_name = %(unique_name)s
               and unix_timestamp(NOW()) < unix_timestamp(expired_at);
            """
            df = sql_to_df(sql=sql, params={"ent_name": dict_in['name']})
            if df is not None and len(df) > 0:
                df.sort_values(by=['expired_at'], ascending=False, inplace=True)
                return int(df['risk_subject_id'].iloc[0])
        return None

    # 读取 info_audience_tag 主键数据
    def _load_info_audience_tag_id(self, dict_in) -> int:
        sql = """               
                      SELECT *
                       FROM info_audience_tag WHERE mobile = %(mobile)s;
        """
        info_audience_tag_df = sql_to_df(sql=sql, params={"mobile": dict_in['phone']})
        if info_audience_tag_df is not None and len(info_audience_tag_df) > 0:
            info_audience_tag_df.sort_values(by=['expired_at'], ascending=False, inplace=True)
            return int(info_audience_tag_df['id'].iloc[0])
        return None

    # 读取 info_court_tax_arrears 数据
    def _load_info_court_tax_arrears_df(self, id) -> pd.DataFrame:
        sql = """
               SELECT * FROM info_court_tax_arrears WHERE risk_subject_id = %(id)s and unix_timestamp(NOW()) < unix_timestamp(expired_at);
        """
        info_court_tax_arrears_df = sql_to_df(sql=sql, params={"id": id})
        if info_court_tax_arrears_df is not None and len(info_court_tax_arrears_df) > 0:
            return info_court_tax_arrears_df
        return None

    # 读取 info_court_criminal_suspect 数据
    def _load_info_court_criminal_suspect_df(self, id) -> pd.DataFrame:
        sql = """
                SELECT * FROM info_court_criminal_suspect WHERE risk_subject_id = %(id)s and unix_timestamp(NOW()) < unix_timestamp(expired_at);
        """
        info_court_criminal_suspect_df = sql_to_df(sql=sql, params={"id": id})
        if info_court_criminal_suspect_df is not None and len(info_court_criminal_suspect_df) > 0:
            return info_court_criminal_suspect_df
        return pd.DataFrame(columns=['id', 'court_id', 'risk_subject_id', 'expired_at', 'channel_api_no',
                                     'id_no', 'name', 'objection', 'trial_date', 'criminal_reason',
                                     'case_no', 'trial_authority', 'trial_result'])

    # 读取 info_court_deadbeat 数据
    def _load_info_court_deadbeat_df(self, id) -> pd.DataFrame:
        sql = """
                SELECT * FROM info_court_deadbeat WHERE risk_subject_id = %(id)s and execute_status <> "已结案" and unix_timestamp(NOW()) < unix_timestamp(expired_at);
        """
        info_court_deadbeat_df = sql_to_df(sql=sql, params={"id": id})
        if info_court_deadbeat_df is not None and len(info_court_deadbeat_df) > 0:
            return info_court_deadbeat_df
        return pd.DataFrame(columns=['id', 'court_id', 'risk_subject_id', 'expired_at', 'channel_api_no',
                                     'id_no', 'name', 'objection', 'trial_date', 'criminal_reason',
                                     'case_no', 'trial_authority', 'trial_result'])

    # 读取 info_court_limit_hignspending 数据
    def _load_info_court_limit_hignspending_df(self, id) -> pd.DataFrame:
        sql = """
                SELECT * FROM info_court_limit_hignspending WHERE risk_subject_id = %(id)s and unix_timestamp(NOW()) < unix_timestamp(expired_at);
        """
        info_court_limit_hignspending_df = sql_to_df(sql=sql, params={"id": id})
        if info_court_limit_hignspending_df is not None and len(info_court_limit_hignspending_df) > 0:
            return info_court_limit_hignspending_df
        return pd.DataFrame(columns=['id', 'court_id', 'risk_subject_id', 'expired_at', 'channel_api_no',
                                     'id_no', 'name', 'objection', 'trial_date', 'criminal_reason',
                                     'case_no', 'trial_authority', 'trial_result'])

    # 读取 info_court_limited_entry_exit 数据
    def _load_info_court_limited_entry_exit_df(self, id) -> pd.DataFrame:
        sql = """
                SELECT * FROM info_court_limited_entry_exit WHERE risk_subject_id = %(id)s and unix_timestamp(NOW()) < unix_timestamp(expired_at);
        """
        info_info_court_limited_entry_exit_df = sql_to_df(sql=sql, params={"id": id})
        if info_info_court_limited_entry_exit_df is not None and len(info_info_court_limited_entry_exit_df) > 0:
            return info_info_court_limited_entry_exit_df
        return pd.DataFrame(columns=['id', 'court_id', 'risk_subject_id', 'expired_at', 'channel_api_no',
                                     'id_no', 'name', 'objection', 'trial_date', 'criminal_reason',
                                     'case_no', 'trial_authority', 'trial_result'])

    # 读取 info_com_bus_entinvitem 数据
    def _load_info_com_bus_entinvitem_df(self, id) -> pd.DataFrame:
        sql = """
                          SELECT *
               FROM info_risk_factor_item;
        """
        info_com_bus_entinvitem_df = sql_to_df(sql=sql, params={"id": id})
        if info_com_bus_entinvitem_df is not None and len(info_com_bus_entinvitem_df) > 0:
            return info_com_bus_entinvitem_df
        return []

    # 读取 info_audience_tag_item 数据
    def _load_info_audience_tag_item_df(self, id) -> pd.DataFrame:
        if id is not None:
            sql = """
                   SELECT * FROM info_audience_tag_item WHERE audience_tag_id = %(id)s and unix_timestamp(NOW()) < unix_timestamp(expired_at);
            """
            info_audience_tag_item_df = sql_to_df(sql=sql, params={"id": id})
            if info_audience_tag_item_df is not None and len(info_audience_tag_item_df) > 0:
                dff = DataFrameFlatter(info_audience_tag_item_df, 'audience_tag_id', 'field_name', 'field_value')
                return dff.flat_df()
        return None

    # 计算 owner_tax 相关字段
    def _owner_tax(self, df=None):
        if df is not None and len(df) > 0:
            df = df.drop_duplicates().sort_values(by=['taxes_time'], ascending=False)
            self.variables['owner_tax_cnt'] = len(df)
            self.variables['owner_tax_name'] = df['name'].to_list()
            self.variables['owner_tax_amt'] = df['taxes'].to_list()
            self.variables['owner_tax_type'] = df['taxes_type'].to_list()
            self.variables['owner_tax_date'] = df['taxes_time'].to_list()

    # 计算 owner_list 相关字段
    def _owner_list(self, df1=None, df2=None, df3=None, df4=None):
        self.variables['owner_list_cnt'] = len(
            df1['name'].to_list() + df2['name'].to_list() + df3['name'].to_list() + df4['name'].to_list())
        self.variables['owner_list_name'] = df1['name'].to_list() + df2['name'].to_list() + df3['name'].to_list() + df4[
            'name'].to_list()
        self.variables['owner_list_style'] = ['罪犯及嫌疑人'] * len(df1['name']) + ['失信老赖'] * len(
            df2['name']) + ['限制高消费'] * len(df3['name']) + ['限制出入境'] * len(df4['name'])
        self.variables['owner_list_detail'] = (df1['case_no'] + df1['trial_result']).to_list() + (
                df2['execute_case_no'] + df2['execute_content']).to_list() + (
                                                      df3['execute_case_no'] + df3['execute_content']).to_list() + (
                                                      df4['execute_no'] + df4['execute_content']).to_list()

    # 计算 jg_v5 相关字段
    def _owner_app(self, df_in):

        def nona_mean(x):
            return np.round(0, 1) if len(x) == 0 else np.round(x.mean(), 1)

        all_fileds = """APP_HOBY_BUS、APP_HOBY_TICKET、APP_HOBY_TRAIN、APP_HOBY_FLIGHT、APP_HOBY_TAXI、APP_HOBY_SPECIAL_DRIVE、APP_HOBY_HIGH_BUS、APP_HOBY_OTHER_DRIVE、APP_HOBY_RENT_CAR
                APP_HOBY_YOUNG_HOTEL、APP_HOBY_HOME_HOTEL、APP_HOBY_CONVERT_HOTEL
                APP_HOBY_BANK_UNIN、APP_HOBY_THIRD_PAY、APP_HOBY_INTERNET_BANK、APP_HOBY_FOREIGN_BANK、APP_HOBY_MIDDLE_BANK、APP_HOBY_CREDIT_CARD、APP_HOBY_CITY_BANK、APP_HOBY_STATE_BANK
                APP_HOBY_FUTURES、APP_HOBY_VIRTUAL_CURRENCY、APP_HOBY_FOREX、APP_HOBY_NOBLE_METAL、APP_HOBY_FUND、APP_HOBY_STOCK、APP_HOBY_ZONGHELICAI
                APP_HOBY_SPORT_LOTTERY、APP_HOBY_WELFARE_LOTTERY、APP_HOBY_DOUBLE_BALL、APP_HOBY_LOTTERY、APP_HOBY_FOOTBALL_LOTTERY
                APP_HOBY_SUMMARY_LIVE、APP_HOBY_SHORT_VIDEO、APP_HOBY_SOCIAL_LIVE、APP_HOBY_SUMMARY_VIDEO、APP_HOBY_SPORTS_VIDEO、APP_HOBY_GAME_LIVE、APP_HOBY_SELF_PHOTO、APP_HOBY_TV_LIVE、APP_HOBY_CULTURE_LIVE、APP_HOBY_SHOW_LIVE、APP_HOBY_SPORTS_LIVE
                APP_HOBY_READ_LISTEN、APP_HOBY_SUNMMARY_NEWS、APP_HOBY_WOMEN_HEL_BOOK、APP_HOBY_ARMY_NEWS、APP_HOBY_CARTON_BOOK、APP_HOBY_PHY_NEWS、APP_HOBY_FAMOUSE_BOOK、APP_HOBY_FINCAL_NEWS、APP_HOBY_FUN_NEWS、APP_HOBY_EDU_MED、APP_HOBY_KONGFU、APP_HOBY_TECH_NEWS、APP_HOBY_LOOK_FOR_MED、APP_HOBY_ENCOURAGE_BOOK、APP_HOBY_CAR_INFO_NEWS、APP_HOBY_HUMERIOUS
                APP_HOBY_CARDS_GAME、APP_HOBY_SPEED_GAME、APP_HOBY_ROLE_GAME、APP_HOBY_NET_GAME、APP_HOBY_RELAX_GAME、APP_HOBY_KONGFU_GAME、APP_HOBY_GAME_VIDEO、APP_HOBY_TALE_GAME、APP_HOBY_DIAMONDS_GAME、APP_HOBY_TRAGEDY_GAME
                APP_HOBY_OUTDOOR、APP_HOBY_MOVIE、APP_HOBY_CARTON、APP_HOBY_BEAUTIFUL、APP_HOBY_LOSE_WEIGHT、APP_HOBY_PHY_BOOK、APP_HOBY_FRESH_SHOPPING、APP_HOBY_WIFI、APP_HOBY_CAR_PRO、APP_HOBY_LIFE_PAY、APP_HOBY_PET_MARKET、APP_HOBY_OUT_FOOD、APP_HOBY_FOOD、APP_HOBY_PALM_MARKET、APP_HOBY_WOMEN_HEAL、APP_HOBY_RECORD、APP_HOBY_CONCEIVE、APP_HOBY_SHARE、APP_HOBY_COOK_BOOK、APP_HOBY_BUY_RENT_HOUSE、APP_HOBY_CHINESE_MEDICINE、APP_HOBY_JOB、APP_HOBY_HOME_SERVICE、APP_HOBY_KRAYOK、APP_HOBY_FAST_SEND
                APP_HOBY_PEOPLE_RESOUSE、APP_HOBY_MAMA_SOCIAL、APP_HOBY_HOT_SOCIAL、APP_HOBY_MARRY_SOCIAL、APP_HOBY_CAMPUS_SOCIAL、APP_HOBY_LOVERS_SOCIAL、APP_HOBY_ECY、APP_HOBY_STRANGER_SOCIAL、APP_HOBY_ANONYMOUS_SOCIAL、APP_HOBY_CITY_SOCIAL、APP_HOBY_FANS
                APP_HOBY_FIN、APP_HOBY_MIDDLE、APP_HOBY_IT、APP_HOBY_PRIMARY、APP_HOBY_BABY、APP_HOBY_ONLINE_STUDY、APP_HOBY_FOREIGN、APP_HOBY_DRIVE、APP_HOBY_SERVANTS、APP_HOBY_CHILD_EDU、APP_HOBY_UNIVERSITY
                APP_HOBY_CAR_SHOPPING、APP_HOBY_SECONDHAND_SHOPPING、APP_HOBY_ZONGHE_SHOPPING、APP_HOBY_PAYBACK、APP_HOBY_DISCOUNT_MARKET、APP_HOBY_BABY_SHOPPING、APP_HOBY_WOMEN_SHOPPING、APP_HOBY_REBATE_SHOPPING、APP_HOBY_GROUP_BUY、APP_HOBY_GLOBAL_SHOPPING、APP_HOBY_SHOPPING_GUIDE、APP_HOBY_SEX_SHOPPING
                APP_HOBY_SMOTE_OFFICE
                APP_HOBY_CAR_LOAN、APP_HOBY_DIVIDE_LOAN、APP_HOBY_CREDIT_CARD_LOAN、APP_HOBY_CASH_LOAN、APP_HOBY_HOUSE_LOAN、APP_HOBY_P2P、APP_HOBY_LOAN_PLATFORM
                APP_HOBY_CAR_LOAN
                APP_HOBY_DIVIDE_LOAN
                APP_HOBY_CREDIT_CARD_LOAN
                APP_HOBY_CASH_LOAN
                APP_HOBY_HOUSE_LOAN
                APP_HOBY_P2P
                APP_HOBY_LOAN_PLATFORM
                """.split()

        filed_list = []
        for i in all_fileds:
            if "、" in i:
                filed_list += i.split("、")
            else:
                filed_list.append(i)

        if df_in[set(df_in.columns.values).intersection(filed_list)].loc[0].isnull().sum() == len(
                (set(df_in.columns.values).intersection(filed_list))):
            self.variables['owner_app_cnt'] = 0
            return
        else:
            self.variables['owner_app_cnt'] = 1

        owner_app_traffic = all_fileds[0].split("、")
        owner_app_traffic_set = set(df_in.columns.values).intersection(set(owner_app_traffic))
        self.variables['owner_app_traffic'] = nona_mean(
            df_in[owner_app_traffic_set].loc[0].str.extractall(r"{score=(.*?), value=(.*?)}").loc[:, 0].map(float))

        owner_app_hotel = all_fileds[1].split("、")
        owner_app_hotel_set = set(df_in.columns.values).intersection(set(owner_app_hotel))
        self.variables['owner_app_hotel'] = nona_mean(
            df_in[owner_app_hotel_set].loc[0].str.extractall(r"{score=(.*?), value=(.*?)}").loc[:, 0].map(float))

        owner_app_finance = all_fileds[2].split("、")
        owner_app_finance_set = set(df_in.columns.values).intersection(set(owner_app_finance))
        self.variables['owner_app_finance'] = nona_mean(
            df_in[owner_app_finance_set].loc[0].str.extractall(r"{score=(.*?), value=(.*?)}").loc[:, 0].map(float))

        owner_app_invest = all_fileds[3].split("、")
        owner_app_invest_set = set(df_in.columns.values).intersection(set(owner_app_invest))
        self.variables['owner_app_invest'] = nona_mean(
            df_in[owner_app_invest_set].loc[0].str.extractall(r"{score=(.*?), value=(.*?)}").loc[:, 0].map(float))

        owner_app_ent = all_fileds[4].split("、")
        owner_app_ent_set = set(df_in.columns.values).intersection(set(owner_app_ent))
        self.variables['owner_app_ent'] = nona_mean(
            df_in[owner_app_ent_set].loc[0].str.extractall(r"{score=(.*?), value=(.*?)}").loc[:, 0].map(float))

        owner_app_live = all_fileds[5].split("、")
        owner_app_live_set = set(df_in.columns.values).intersection(set(owner_app_live))
        self.variables['owner_app_live'] = nona_mean(
            df_in[owner_app_live_set].loc[0].str.extractall(r"{score=(.*?), value=(.*?)}").loc[:, 0].map(float))

        owner_app_read = all_fileds[6].split("、")
        owner_app_read_set = set(df_in.columns.values).intersection(set(owner_app_read))
        self.variables['owner_app_read'] = nona_mean(
            df_in[owner_app_read_set].loc[0].str.extractall(r"{score=(.*?), value=(.*?)}").loc[:, 0].map(float))

        owner_app_game = all_fileds[7].split("、")
        owner_app_game_set = set(df_in.columns.values).intersection(set(owner_app_game))
        self.variables['owner_app_game'] = nona_mean(
            df_in[owner_app_game_set].loc[0].str.extractall(r"{score=(.*?), value=(.*?)}").loc[:, 0].map(float))

        owner_app_life = all_fileds[8].split("、")
        owner_app_life_set = set(df_in.columns.values).intersection(set(owner_app_life))
        self.variables['owner_app_life'] = nona_mean(
            df_in[owner_app_life_set].loc[0].str.extractall(r"{score=(.*?), value=(.*?)}").loc[:, 0].map(float))

        owner_app_social = all_fileds[9].split("、")
        owner_app_social_set = set(df_in.columns.values).intersection(set(owner_app_social))
        self.variables['owner_app_social'] = nona_mean(
            df_in[owner_app_social_set].loc[0].str.extractall(r"{score=(.*?), value=(.*?)}").loc[:, 0].map(float))

        owner_app_edu = all_fileds[10].split("、")
        owner_app_edu_set = set(df_in.columns.values).intersection(set(owner_app_edu))
        self.variables['owner_app_edu'] = nona_mean(
            df_in[owner_app_edu_set].loc[0].str.extractall(r"{score=(.*?), value=(.*?)}").loc[:, 0].map(float))

        owner_app_shop = all_fileds[11].split("、")
        owner_app_shop_set = set(df_in.columns.values).intersection(set(owner_app_shop))
        self.variables['owner_app_shop'] = nona_mean(
            df_in[owner_app_shop_set].loc[0].str.extractall(r"{score=(.*?), value=(.*?)}").loc[:, 0].map(float))

        owner_app_work = all_fileds[12].split("、")
        owner_app_work_set = set(df_in.columns.values).intersection(set(owner_app_work))
        self.variables['owner_app_work'] = nona_mean(
            df_in[owner_app_work_set].loc[0].str.extractall(r"{score=(.*?), value=(.*?)}").loc[:, 0].map(float))

        owner_app_loan = all_fileds[13].split("、")
        owner_app_loan_set = set(df_in.columns.values).intersection(set(owner_app_loan))
        self.variables['owner_app_loan'] = nona_mean(
            df_in[owner_app_loan_set].loc[0].str.extractall(r"{score=(.*?), value=(.*?)}").loc[:, 0].map(float))

        owner_app_loan_car = all_fileds[14].split("、")
        owner_app_loan_car_set = set(df_in.columns.values).intersection(set(owner_app_loan_car))
        self.variables['owner_app_loan_car'] = nona_mean(
            df_in[owner_app_loan_car_set].loc[0].str.extractall(r"{score=(.*?), value=(.*?)}").loc[:, 0].map(float))

        owner_app_loan_installment = all_fileds[15].split("、")
        owner_app_loan_installment_set = set(df_in.columns.values).intersection(set(owner_app_loan_installment))
        self.variables['owner_app_loan_installment'] = nona_mean(
            df_in[owner_app_loan_installment_set].loc[0].str.extractall(r"{score=(.*?), value=(.*?)}").loc[:, 0].map(
                float))

        owner_app_loan_credit = all_fileds[16].split("、")
        owner_app_loan_credit_set = set(df_in.columns.values).intersection(set(owner_app_loan_credit))
        self.variables['owner_app_loan_credit'] = nona_mean(
            df_in[owner_app_loan_credit_set].loc[0].str.extractall(r"{score=(.*?), value=(.*?)}").loc[:, 0].map(float))

        owner_app_loan_cash = all_fileds[17].split("、")
        owner_app_loan_cash_set = set(df_in.columns.values).intersection(set(owner_app_loan_cash))
        self.variables['owner_app_loan_cash'] = nona_mean(
            df_in[owner_app_loan_cash_set].loc[0].str.extractall(r"{score=(.*?), value=(.*?)}").loc[:, 0].map(float))

        owner_app_loan_house = all_fileds[18].split("、")
        owner_app_loan_house_set = set(df_in.columns.values).intersection(set(owner_app_loan_house))
        self.variables['owner_app_loan_house'] = nona_mean(
            df_in[owner_app_loan_house_set].loc[0].str.extractall(r"{score=(.*?), value=(.*?)}").loc[:, 0].map(float))

        owner_app_loan_p2p = all_fileds[19].split("、")
        owner_app_loan_p2p_set = set(df_in.columns.values).intersection(set(owner_app_loan_p2p))
        self.variables['owner_app_loan_p2p'] = nona_mean(
            df_in[owner_app_loan_p2p_set].loc[0].str.extractall(r"{score=(.*?), value=(.*?)}").loc[:, 0].map(float))

        owner_app_loan_platform = all_fileds[20].split("、")
        owner_app_loan_platform_set = set(df_in.columns.values).intersection(set(owner_app_loan_platform))
        self.variables['owner_app_loan_platform'] = nona_mean(
            df_in[owner_app_loan_platform_set].loc[0].str.extractall(r"{score=(.*?), value=(.*?)}").loc[:, 0].map(
                float))

    # 计算 企业主个人信息
    def _owner_info(self,df_in):
        base = (pd.datetime.now().year - pd.to_datetime(df_in['idno'][6:14]).year) - 1
        remainder = 0 if (pd.datetime(year = int(df_in['idno'][6:14][:4]), month=pd.datetime.now().month, day=pd.datetime.now().day) - pd.to_datetime(df_in['idno'][6:14])).days < 0 else 1
        self.variables['owner_age'] = base + remainder

    def transform(self):
        query_list = self._jsonpath_load(self.full_msg)
        for each in query_list:
            risk_subject_id = self._load_info_com_bus_basic_id(each)

            if each['baseType'].upper() == 'PERSONAL':
                jg_id = self._load_info_audience_tag_id(each)
                df = self._load_info_audience_tag_item_df(jg_id)
                self._owner_app(df)

            elif each['baseType'].upper() == "COMPANY":
                df = self._load_info_court_tax_arrears_df(risk_subject_id)
                self._owner_tax(df)

            df1 = self._load_info_court_criminal_suspect_df(risk_subject_id)
            df2 = self._load_info_court_deadbeat_df(risk_subject_id)
            df3 = self._load_info_court_limit_hignspending_df(risk_subject_id)
            df4 = self._load_info_court_limited_entry_exit_df(risk_subject_id)
            self._owner_list(df1, df2, df3, df4)

