from mapping.tranformer import Transformer
from util.mysql_reader import sql_to_df
import pandas as pd


class V16002(Transformer):
    """
    法院核查:贷前+贷后详细信息展示
    """
    def __init__(self):
        super().__init__()
        self.variables = {
            'info_court_ent_owed_owe': None,  # 法院核查_企业_欠款欠费名单_贷后+贷前
            'info_court_ent_dishonesty': None,  # 法院核查_企业_失信老赖名单_贷后+贷前
            'info_court_ent_limit_entry': None,  # 法院核查_企业_限制出入境名单_贷后+贷前
            'info_court_ent_high_cons': None,  # 法院核查_企业_限制高消费名单_贷后+贷前
            'info_court_ent_cri_sus': None,  # 法院核查_企业_罪犯及嫌疑人名单_贷后+贷前
            'info_court_ent_fin_loan_con': None,  # 法院核查_企业_金融借款合同纠纷_贷后+贷前
            'info_court_ent_loan_con': None,  # 法院核查_企业_借款合同纠纷_贷后+贷前
            'info_court_ent_pop_loan': None,  # 法院核查_企业_民间借贷纠纷_贷后+贷前
            'info_court_ent_admi_vio': None,  # 法院核查_企业_行政违法记录_贷后+贷前
            'info_court_ent_judge': None,  # 法院核查_企业_民商事裁判文书_贷后+贷前
            'info_court_ent_trial_proc': None,  # 法院核查_企业_民商事审判流程_贷后+贷前
            'info_court_ent_tax_pay': None,  # 法院核查_企业_纳税非正常户_贷后+贷前
            'info_court_ent_pub_info': None,  # 法院核查_企业_执行公开信息_贷后+贷前
            'info_court_ent_tax_arrears': None  # 法院核查_企业_欠税名单_贷后+贷前
        }
        self.pre_biz_date = None
        self.user_name = None
        self.id_card_no = None

    # 命中各类名单详细信息展示
    def _hit_list_details(self):
        hit_list = {
            'info_court_ent_owed_owe': 'info_court_arrearage',  # 法院核查_企业_欠款欠费名单_贷后+贷前
            'info_court_ent_dishonesty': 'info_court_deadbeat',  # 法院核查_企业_失信老赖名单_贷后+贷前
            'info_court_ent_limit_entry': 'info_court_limited_entry_exit',  # 法院核查_企业_限制出入境名单_贷后+贷前
            'info_court_ent_high_cons': 'info_court_limit_hignspending',  # 法院核查_企业_限制高消费名单_贷后+贷前
            'info_court_ent_cri_sus': 'info_court_criminal_suspect',  # 法院核查_企业_罪犯及嫌疑人名单_贷后+贷前
            'info_court_ent_admi_vio': 'info_court_administrative_violation',  # 法院核查_企业_行政违法记录_贷后+贷前
            'info_court_ent_judge': 'info_court_judicative_pape',  # 法院核查_企业_民商事裁判文书_贷后+贷前
            'info_court_ent_trial_proc': 'info_court_trial_process',  # 法院核查_企业_民商事审判流程_贷后+贷前
            'info_court_ent_tax_pay': 'info_court_taxable_abnormal_user',  # 法院核查_企业_纳税非正常户_贷后+贷前
            'info_court_ent_pub_info': 'info_court_excute_public',  # 法院核查_企业_执行公开信息_贷后+贷前
            'info_court_ent_tax_arrears': 'info_court_tax_arrears'  # 法院核查_企业_欠税名单_贷后+贷前
        }
        for var in hit_list.keys():
            sql_before_loan = """
                select 
                    a.*
                from 
                    %s a """ % hit_list[var] + """
                left join 
                    (select id FROM info_court where create_time < %(result_date)s
                    and (unique_name=%(user_name)s or unique_id_no=%(id_card_no)s) 
                    order by create_time desc limit 1) b 
                on 
                    a.court_id=b.id
                """
            sql_after_loan = """
                select 
                    a.*
                from 
                    %s a """ % hit_list[var] + """
                left join 
                    (select id FROM info_court where create_time < NOW()
                    and (unique_name=%(user_name)s or unique_id_no=%(id_card_no)s) 
                    order by create_time desc limit 1) b 
                on 
                    a.court_id=b.id
                """
            df_before_loan = sql_to_df(sql=sql_before_loan,
                                       params={'result_date': self.pre_biz_date,
                                               'user_name': self.user_name,
                                               'id_card_no': self.id_card_no})
            df_after_loan = sql_to_df(sql=sql_after_loan,
                                      params={'user_name': self.user_name,
                                              'id_card_no': self.id_card_no})
            self.variables[var] = {'before': [], 'after': []}
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

    # 金融借款合同纠纷和民间借贷纠纷详细信息展示
    def _hit_contract_dispute_details(self):
        hit_list = {
            'info_court_ent_fin_loan_con': '金融借款合同纠纷',  # 法院核查_企业_金融借款合同纠纷_贷后+贷前
            'info_court_ent_pop_loan': '民间借贷纠纷'  # 法院核查_企业_民间借贷纠纷_贷后+贷前
        }
        for var in hit_list.keys():
            sql_before_loan1 = """
                select 
                    a.*
                from 
                    info_court_judicative_pape a 
                left join 
                    (select id FROM info_court where create_time < %(result_date)s
                    and (unique_name=%(user_name)s or unique_id_no=%(id_card_no)s) 
                    order by create_time desc limit 1) b 
                on 
                    a.court_id=b.id
                where
                    a.case_reason like %(case_reason)s and a.legal_status like '%%被告%%'
                """
            sql_after_loan1 = """
                select 
                    a.*
                from 
                    info_court_judicative_pape a 
                left join 
                    (select id FROM info_court where create_time < NOW()
                    and (unique_name=%(user_name)s or unique_id_no=%(id_card_no)s)
                    order by create_time desc limit 1) b 
                on 
                    a.court_id=b.id
                where
                    a.case_reason like %(case_reason)s and a.legal_status like '%%被告%%'
                """
            sql_before_loan2 = """
                select 
                    a.*
                from 
                    info_court_trial_process a 
                left join 
                    (select id FROM info_court where create_time < %(result_date)s
                    and (unique_name=%(user_name)s or unique_id_no=%(id_card_no)s)
                    order by create_time desc limit 1) b 
                on 
                    a.court_id=b.id
                where
                    a.case_reason like %(case_reason)s and a.legal_status like '%%被告%%'
                """
            sql_after_loan2 = """
                select 
                    a.*
                from 
                    info_court_trial_process a 
                left join 
                    (select id FROM info_court where create_time < NOW()
                    and (unique_name=%(user_name)s or unique_id_no=%(id_card_no)s)
                    order by create_time desc limit 1) b 
                on 
                    a.court_id=b.id
                where
                    a.case_reason like %(case_reason)s and a.legal_status like '%%被告%%'
                """
            df_before_loan1 = sql_to_df(sql=sql_before_loan1,
                                        params={'result_date': self.pre_biz_date,
                                                'user_name': self.user_name,
                                                'id_card_no': self.id_card_no,
                                                'case_reason': '%'+hit_list[var]+'%'})
            df_after_loan1 = sql_to_df(sql=sql_after_loan1,
                                       params={'user_name': self.user_name,
                                               'id_card_no': self.id_card_no,
                                               'case_reason': '%'+hit_list[var]+'%'})
            df_before_loan2 = sql_to_df(sql=sql_before_loan2,
                                        params={'result_date': self.pre_biz_date,
                                                'user_name': self.user_name,
                                                'id_card_no': self.id_card_no,
                                                'case_reason': '%'+hit_list[var]+'%'})
            df_after_loan2 = sql_to_df(sql=sql_after_loan2,
                                       params={'user_name': self.user_name,
                                               'id_card_no': self.id_card_no,
                                               'case_reason': '%'+hit_list[var]+'%'})
            self.variables[var] = {'before': [], 'after': []}
            if len(df_before_loan1) > 0:
                for row in df_before_loan1.itertuples():
                    self.variables[var]['before'].append({})
                    for col in df_before_loan1.columns:
                        self.variables[var]['before'][-1][col] = getattr(row, col)
            if len(df_after_loan1) > 0:
                for row in df_after_loan1.itertuples():
                    self.variables[var]['after'].append({})
                    for col in df_after_loan1.columns:
                        self.variables[var]['after'][-1][col] = getattr(row, col)
            if len(df_before_loan2) > 0:
                for row in df_before_loan2.itertuples():
                    self.variables[var]['before'].append({})
                    for col in df_before_loan2.columns:
                        self.variables[var]['before'][-1][col] = getattr(row, col)
            if len(df_after_loan2) > 0:
                for row in df_after_loan2.itertuples():
                    self.variables[var]['after'].append({})
                    for col in df_after_loan2.columns:
                        self.variables[var]['after'][-1][col] = getattr(row, col)
        return

    # 借款合同纠纷详细信息展示
    def _info_court_ent_loan_con(self):
        sql_before_loan1 = """
            select 
                a.*
            from 
                info_court_judicative_pape a 
            left join 
                (select id FROM info_court where create_time < %(result_date)s
                and (unique_name=%(user_name)s or unique_id_no=%(id_card_no)s) 
                order by create_time desc limit 1) b 
            on 
                a.court_id=b.id
            where
                a.case_reason regexp '借款合同纠纷|民间借贷纠纷|金融不良债权追偿纠纷|
                                金融不良债权转让合同纠纷|企业借贷纠纷|同业拆借纠纷'
                and a.legal_status like '%%被告%%'
            """
        sql_after_loan1 = """
            select 
                a.*
            from 
                info_court_judicative_pape a 
            left join 
                (select id FROM info_court where create_time < NOW()
                and (unique_name=%(user_name)s or unique_id_no=%(id_card_no)s)
                order by create_time desc limit 1) b 
            on 
                a.court_id=b.id
            where
                a.case_reason regexp '借款合同纠纷|民间借贷纠纷|金融不良债权追偿纠纷|
                                金融不良债权转让合同纠纷|企业借贷纠纷|同业拆借纠纷'
                and a.legal_status like '%%被告%%'
            """
        sql_before_loan2 = """
            select 
                a.*
            from 
                info_court_trial_process a 
            left join 
                (select id FROM info_court where create_time < %(result_date)s
                and (unique_name=%(user_name)s or unique_id_no=%(id_card_no)s)
                order by create_time desc limit 1) b 
            on 
                a.court_id=b.id
            where
                a.case_reason regexp '借款合同纠纷|民间借贷纠纷|金融不良债权追偿纠纷|
                                金融不良债权转让合同纠纷|企业借贷纠纷|同业拆借纠纷'
                and a.legal_status like '%%被告%%'
            """
        sql_after_loan2 = """
            select 
                a.*
            from 
                info_court_trial_process a 
            left join 
                (select id FROM info_court where create_time < NOW()
                and (unique_name=%(user_name)s or unique_id_no=%(id_card_no)s)
                order by create_time desc limit 1) b 
            on 
                a.court_id=b.id
            where
                a.case_reason regexp '借款合同纠纷|民间借贷纠纷|金融不良债权追偿纠纷|
                                金融不良债权转让合同纠纷|企业借贷纠纷|同业拆借纠纷'
                and a.legal_status like '%%被告%%'
            """
        df_before_loan1 = sql_to_df(sql=sql_before_loan1,
                                    params={'result_date': self.pre_biz_date,
                                            'user_name': self.user_name,
                                            'id_card_no': self.id_card_no})
        df_after_loan1 = sql_to_df(sql=sql_after_loan1,
                                   params={'user_name': self.user_name,
                                           'id_card_no': self.id_card_no})
        df_before_loan2 = sql_to_df(sql=sql_before_loan2,
                                    params={'result_date': self.pre_biz_date,
                                            'user_name': self.user_name,
                                            'id_card_no': self.id_card_no})
        df_after_loan2 = sql_to_df(sql=sql_after_loan2,
                                   params={'user_name': self.user_name,
                                           'id_card_no': self.id_card_no})
        self.variables['info_court_ent_loan_con'] = {'before': [], 'after': []}
        if len(df_before_loan1) > 0:
            for row in df_before_loan1.itertuples():
                self.variables['info_court_ent_loan_con']['before'].append({})
                for col in df_before_loan1.columns:
                    self.variables['info_court_ent_loan_con']['before'][-1][col] = getattr(row, col)
        if len(df_after_loan1) > 0:
            for row in df_after_loan1.itertuples():
                self.variables['info_court_ent_loan_con']['after'].append({})
                for col in df_after_loan1.columns:
                    self.variables['info_court_ent_loan_con']['after'][-1][col] = getattr(row, col)
        if len(df_before_loan2) > 0:
            for row in df_before_loan2.itertuples():
                self.variables['info_court_ent_loan_con']['before'].append({})
                for col in df_before_loan2.columns:
                    self.variables['info_court_ent_loan_con']['before'][-1][col] = getattr(row, col)
        if len(df_after_loan2) > 0:
            for row in df_after_loan2.itertuples():
                self.variables['info_court_ent_loan_con']['after'].append({})
                for col in df_after_loan2.columns:
                    self.variables['info_court_ent_loan_con']['after'][-1][col] = getattr(row, col)
        return

    # 执行变量转换
    def transform(self):
        self.pre_biz_date = self.origin_data.get('preBizDate')

        self.variables["variable_product_code"] = "06001"

        self._hit_list_details()
        self._hit_contract_dispute_details()
        self._info_court_ent_loan_con()
