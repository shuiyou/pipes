# @Time : 2020/10/21 3:31 PM 
# @Author : lixiaobo
# @File : owner.py.py 
# @Software: PyCharm
import datetime
import re
import pandas as pd

from mapping.grouped_tranformer import GroupedTransformer, invoke_each
from util.mysql_reader import sql_to_df
from util.common_util import get_query_data, get_all_related_company


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
            'owner_info_cnt': 1,
            'owner_tax_cnt': 0,
            'owner_list_cnt': 0,
            'owner_app_cnt': 0,
            'owner_age': 0,
            'owner_resistence': '',
            'owner_marriage_status': '',
            'owner_education': '',
            'owner_criminal_score_level': '',
            'owner_list_name': [],
            'owner_list_type': [],
            'owner_list_case_no': [],
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
        self.person_list = None
        self.company_list = None
        self.per_type = None

    # 获取对应主体及主体的关联企业对应的欠税信息
    def _info_court_id(self, idno):
        input_info = self.per_type.get(idno)
        if input_info is not None:
            unique_idno = input_info['idno']
            unique_idno_str = ','.join(unique_idno)
            sql = """
                select id 
                from info_court 
                where unique_id_no in (%(unique_id_no)s) 
                and unix_timestamp(NOW()) < unix_timestamp(expired_at)"""
            df = sql_to_df(sql=sql, params={'unique_id_no': unique_idno_str})
            if df is not None and df.shape[0] > 0:
                id_list = df['id'].to_list()
                court_id_list = ','.join([str(x) for x in id_list])
                sql = """
                    select *
                    from info_court_tax_arrears
                    where court_id in (%(court_id)s)
                """
                df = sql_to_df(sql=sql, params={'court_id': court_id_list})
                if df.shape[0] > 0:
                    self.variables['owner_tax_cnt'] = df.shape[0]
                    df.sort_values(by='taxes_time', ascending=False, inplace=True)
                    self.variables['owner_tax_name'] = df['name'].to_list()
                    self.variables['owner_tax_amt'] = df['taxes'].to_list()
                    self.variables['owner_tax_type'] = df['taxes_type'].to_list()
                    self.variables['owner_tax_date'] = df['taxes_time'].to_list()

    # 获取个人基本信息
    def _indiv_base_info(self, idno):
        idno = str(idno)
        now = datetime.datetime.now()
        self.variables['owner_age'] = now.year - int(idno[6:10]) + \
            (now.month - int(idno[10:12]) + (now.day - int(idno[12:14])) // 100) // 100
        self.variables['owner_resistence'] = idno[:6]
        for index in self.person_list:
            temp_id = index.get('idno')
            if str(temp_id) == idno:
                self.variables['owner_marriage_status'] = self.person_list[index].get('marry_state')
                self.variables['owner_education'] = self.person_list[index].get('education')
                break

    # 获取个人公安重点评分
    def _info_criminal_score(self, idno):
        sql = """
            select score 
            from info_criminal_case
            where id_card_no = %(unique_id_no)s
            and unix_timestamp(NOW()) < unix_timestamp(expired_at)
            order by id desc limit 1
        """
        df = sql_to_df(sql=sql, params={'unique_id_no': idno})
        if df.shape[0] == 0:
            return
        score = df['score'].to_list()[0]
        if score is not None and score.isdigit():
            score = float(score)
            if score > 60:
                level = "A"
            elif score == 60:
                level = "B"
            elif score > 20:
                level = "C"
            else:
                level = "D"
            self.variables['owner_criminal_score_level'] = level

    # 不良记录条数和详情
    def _info_bad_behavior_record(self, idno):
        court_sql = """
            select id 
            from info_court 
            where unique_id_no = %(unique_id_no)s and unix_timestamp(NOW()) < unix_timestamp(expired_at)
            order by id desc limit 1
        """
        court_df = sql_to_df(sql=court_sql, params={'unique_id_no': str(idno)})
        if court_df is not None and court_df.shape[0] > 0:
            court_id = court_df['id'].to_list()[0]
            behavior_sql = """
                select name, '罪犯及嫌疑人' as type, case_no as case_no, criminal_reason as detail 
                from info_court_criminal_suspect where court_id = %(court_id)s
                union all 
                select name, '失信老赖' as type, execute_case_no as case_no, execute_content as detail 
                from info_court_deadbeat where court_id = %(court_id)s and execute_status != '已结案'
                union all 
                select name, '限制高消费' as type, execute_case_no as case_no, execute_content as detail 
                from info_court_limit_hignspending where court_id = %(court_id)s
                union all 
                select name, '限制出入境' as type, execute_no as case_no, execute_content as detail 
                from info_court_limited_entry_exit where court_id = %(court_id)s
            """
            df = sql_to_df(sql=behavior_sql, params={'court_id': court_id})
            if df is not None and df.shape[0] > 0:
                self.variables['owner_list_cnt'] = df.shape[0]
                self.variables['owner_list_name'] = df['name'].to_list()
                self.variables['owner_list_type'] = df['type'].to_list()
                self.variables['owner_list_case_no'] = df['case_no'].to_list()
                self.variables['owner_list_detail'] = df['detail'].apply(lambda x: re.sub(r'\s', '', x)).to_list()

    # 获取每个主体的从业年限和主营业年限
    def _info_operation_period(self, idno):
        main_indu = self.full_msg['strategyParam'].get('industry')
        if main_indu is not None:
            temp_idno = self.per_type.get(idno)
            if temp_idno is not None:
                code_str = ','.join(temp_idno['idno'])
                sql = """
                    select * 
                    from info_com_bus_face 
                    where basic_id in (
                        select id 
                        from info_com_bus_basic 
                        where credit_code in (%(credit_code)s) 
                        and unix_timestamp(NOW()) < unix_timestamp(expired_at)
                    )
                """
                df = sql_to_df(sql=sql, params={'credit_code': code_str})
                df = df[pd.notna(df['es_date'])]
                if df.shape[0] == 0:
                    return
                min_es_date = df['es_date'].min()
                self.variables['owner_job_year'] = datetime.datetime.now().year - min_es_date.year
                industry = temp_idno['industry']
                temp_index = [i for i in range(len(industry)) if industry[i] == main_indu]
                if len(temp_index) == 0:
                    return
                temp_code = [str(temp_idno['idno'][x]) for x in temp_index]
                temp_df = df[df['credit_code'].isin(temp_code)]
                if temp_df.shape[0] == 0:
                    return
                temp_min_es_date = df['es_date'].min()
                self.variables['owner_major_job_year'] = datetime.datetime.now().year - temp_min_es_date.year

    # 获取对应客户的极光数据
    def _info_jg_v5(self, mobile):
        jg_mapping = {
            "owner_app_traffic": ["APP_HOBY_BUS", "APP_HOBY_TICKET", "APP_HOBY_TRAIN", "APP_HOBY_FLIGHT",
                                  "APP_HOBY_TAXI", "APP_HOBY_SPECIAL_DRIVE", "APP_HOBY_HIGH_BUS",
                                  "APP_HOBY_OTHER_DRIVE", "APP_HOBY_RENT_CAR"],
            "owner_app_hotel": ["APP_HOBY_YOUNG_HOTEL", "APP_HOBY_HOME_HOTEL", "APP_HOBY_CONVERT_HOTEL"],
            "owner_app_finance": ["APP_HOBY_BANK_UNIN", "APP_HOBY_THIRD_PAY", "APP_HOBY_INTERNET_BANK",
                                  "APP_HOBY_FOREIGN_BANK", "APP_HOBY_MIDDLE_BANK", "APP_HOBY_CREDIT_CARD",
                                  "APP_HOBY_CITY_BANK", "APP_HOBY_STATE_BANK"],
            "owner_app_invest": ["APP_HOBY_FUTURES", "APP_HOBY_VIRTUAL_CURRENCY", "APP_HOBY_FOREX",
                                 "APP_HOBY_NOBLE_METAL", "APP_HOBY_FUND", "APP_HOBY_STOCK", "APP_HOBY_ZONGHELICAI"],
            "owner_app_ent": ["APP_HOBY_SPORT_LOTTERY", "APP_HOBY_WELFARE_LOTTERY", "APP_HOBY_DOUBLE_BALL",
                              "APP_HOBY_LOTTERY", "APP_HOBY_FOOTBALL_LOTTERY"],
            "owner_app_live": ["APP_HOBY_SUMMARY_LIVE", "APP_HOBY_SHORT_VIDEO", "APP_HOBY_SOCIAL_LIVE",
                               "APP_HOBY_SUMMARY_VIDEO", "APP_HOBY_SPORTS_VIDEO", "APP_HOBY_GAME_LIVE",
                               "APP_HOBY_SELF_PHOTO", "APP_HOBY_TV_LIVE", "APP_HOBY_CULTURE_LIVE", "APP_HOBY_SHOW_LIVE",
                               "APP_HOBY_SPORTS_LIVE"],
            "owner_app_read": ["APP_HOBY_READ_LISTEN", "APP_HOBY_SUNMMARY_NEWS", "APP_HOBY_WOMEN_HEL_BOOK",
                               "APP_HOBY_ARMY_NEWS", "APP_HOBY_CARTON_BOOK", "APP_HOBY_PHY_NEWS",
                               "APP_HOBY_FAMOUSE_BOOK", "APP_HOBY_FINCAL_NEWS", "APP_HOBY_FUN_NEWS", "APP_HOBY_EDU_MED",
                               "APP_HOBY_KONGFU", "APP_HOBY_TECH_NEWS", "APP_HOBY_LOOK_FOR_MED",
                               "APP_HOBY_ENCOURAGE_BOOK", "APP_HOBY_CAR_INFO_NEWS", "APP_HOBY_HUMERIOUS"],
            "owner_app_game": ["APP_HOBY_CARDS_GAME", "APP_HOBY_SPEED_GAME", "APP_HOBY_ROLE_GAME", "APP_HOBY_NET_GAME",
                               "APP_HOBY_RELAX_GAME", "APP_HOBY_KONGFU_GAME", "APP_HOBY_GAME_VIDEO",
                               "APP_HOBY_TALE_GAME", "APP_HOBY_DIAMONDS_GAME", "APP_HOBY_TRAGEDY_GAME"],
            "owner_app_life": ["APP_HOBY_OUTDOOR", "APP_HOBY_MOVIE", "APP_HOBY_CARTON", "APP_HOBY_BEAUTIFUL",
                               "APP_HOBY_LOSE_WEIGHT", "APP_HOBY_PHY_BOOK", "APP_HOBY_FRESH_SHOPPING", "APP_HOBY_WIFI",
                               "APP_HOBY_CAR_PRO", "APP_HOBY_LIFE_PAY", "APP_HOBY_PET_MARKET", "APP_HOBY_OUT_FOOD",
                               "APP_HOBY_FOOD", "APP_HOBY_PALM_MARKET", "APP_HOBY_WOMEN_HEAL", "APP_HOBY_RECORD",
                               "APP_HOBY_CONCEIVE", "APP_HOBY_SHARE", "APP_HOBY_COOK_BOOK", "APP_HOBY_BUY_RENT_HOUSE",
                               "APP_HOBY_CHINESE_MEDICINE", "APP_HOBY_JOB", "APP_HOBY_HOME_SERVICE", "APP_HOBY_KRAYOK",
                               "APP_HOBY_FAST_SEND"],
            "owner_app_social": ["APP_HOBY_PEOPLE_RESOUSE", "APP_HOBY_MAMA_SOCIAL", "APP_HOBY_HOT_SOCIAL",
                                 "APP_HOBY_MARRY_SOCIAL", "APP_HOBY_CAMPUS_SOCIAL", "APP_HOBY_LOVERS_SOCIAL",
                                 "APP_HOBY_ECY", "APP_HOBY_STRANGER_SOCIAL", "APP_HOBY_ANONYMOUS_SOCIAL",
                                 "APP_HOBY_CITY_SOCIAL", "APP_HOBY_FANS"],
            "owner_app_edu": ["APP_HOBY_FIN", "APP_HOBY_MIDDLE", "APP_HOBY_IT", "APP_HOBY_PRIMARY", "APP_HOBY_BABY",
                              "APP_HOBY_ONLINE_STUDY", "APP_HOBY_FOREIGN", "APP_HOBY_DRIVE", "APP_HOBY_SERVANTS",
                              "APP_HOBY_CHILD_EDU", "APP_HOBY_UNIVERSITY"],
            "owner_app_shop": ["APP_HOBY_CAR_SHOPPING", "APP_HOBY_SECONDHAND_SHOPPING", "APP_HOBY_ZONGHE_SHOPPING",
                               "APP_HOBY_PAYBACK", "APP_HOBY_DISCOUNT_MARKET", "APP_HOBY_BABY_SHOPPING",
                               "APP_HOBY_WOMEN_SHOPPING", "APP_HOBY_REBATE_SHOPPING", "APP_HOBY_GROUP_BUY",
                               "APP_HOBY_GLOBAL_SHOPPING", "APP_HOBY_SHOPPING_GUIDE", "APP_HOBY_SEX_SHOPPING"],
            "owner_app_work": ["APP_HOBY_SMOTE_OFFICE"],
            "owner_app_loan": ["APP_HOBY_CAR_LOAN", "APP_HOBY_DIVIDE_LOAN", "APP_HOBY_CREDIT_CARD_LOAN",
                               "APP_HOBY_CASH_LOAN", "APP_HOBY_HOUSE_LOAN", "APP_HOBY_P2P", "APP_HOBY_LOAN_PLATFORM"],
            "owner_app_loan_car": ["APP_HOBY_CAR_LOAN"],
            "owner_app_loan_installment": ["APP_HOBY_DIVIDE_LOAN"],
            "owner_app_loan_credit": ["APP_HOBY_CREDIT_CARD_LOAN"],
            "owner_app_loan_cash": ["APP_HOBY_CASH_LOAN"],
            "owner_app_loan_house": ["APP_HOBY_HOUSE_LOAN"],
            "owner_app_loan_p2p": ["APP_HOBY_P2P"],
            "owner_app_loan_platform": ["APP_HOBY_LOAN_PLATFORM"]
        }
        mobile = str(mobile)
        sql = """
            select * 
            from info_audience_tag_item
            where audience_tag_id = (
                select id 
                from info_audience_tag 
                where mobile = %(mobile)s 
                and unix_timestamp(NOW()) < unix_timestamp(expired_at)
                order by id desc limit 1
            )
        """
        jg_df = sql_to_df(sql=sql, params={'mobile': mobile})
        if jg_df.shape[0] == 0:
            return
        self.variables['owner_app_cnt'] = 1
        for k, v in jg_mapping.items():
            temp_df = jg_df[jg_df['field_name'].isin(v)]
            if temp_df.shape[0] == 0:
                continue
            temp_df['field_value'] = temp_df['field_value'].apply(
                lambda x: re.search(r'(?<=score=).*?(?=,)', x).group())
            temp_df['field_value'] = temp_df['field_value'].fillna(0).apply(float)
            self.variables[k] = round(temp_df['field_value'].mean() * 100, 1)

    def transform(self):
        self.person_list = get_query_data(self.full_msg, 'PERSONAL', '01')
        self.company_list = get_query_data(self.full_msg, 'COMPANY', '01')
        self.per_type = get_all_related_company(self.full_msg)
        id_no = self.id_card_no
        base_type = self.base_type
        phone = self.phone
        # for index in self.person_list:
        #     temp_id = index.get('idno')
        #     if str(temp_id) == id_no:
        #         phone = self.person_list[temp_id].get('phone')
        #         break
        if "PERSONAL" in base_type:
            self._indiv_base_info(id_no)
            self._info_court_id(id_no)
            self._info_criminal_score(id_no)
            self._info_bad_behavior_record(id_no)
            self._info_operation_period(id_no)
            self._info_jg_v5(phone)
