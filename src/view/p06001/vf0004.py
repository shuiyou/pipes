import pandas as pd
from mapping.tranformer import Transformer
from util.mysql_reader import sql_to_df


class Vf0004(Transformer):
    """
    企业工商法院核查,贷前+贷后详细信息展示
    """

    def __init__(self):
        super().__init__()
        self.variables = {
            'info_com_bus_court_open_owed_owe': None,  # 工商法院_在营企业_欠款欠费名单_贷后+贷前
            'info_com_bus_court_open_court_dishonesty': None,  # 工商法院_在营企业_失信老赖名单_贷后+贷前
            'info_com_bus_court_open_rest_entry': None,  # 工商法院_在营企业_限制出入境名单_贷后+贷前
            'info_com_bus_court_open_high_cons': None,  # 工商法院_在营企业_限制高消费名单_贷后+贷前
            'info_com_bus_court_open_cri_sus': None,  # 工商法院_在营企业_罪犯及嫌疑人名单_贷后+贷前
            'info_com_bus_court_open_fin_loan_con': None,  # 工商法院_在营企业_金融借款合同纠纷_贷后+贷前
            'info_com_bus_court_open_loan_con': None,  # 工商法院_在营企业_借款合同纠纷_贷后+贷前
            'info_com_bus_court_open_pop_loan': None,  # 工商法院_在营企业_民间借贷纠纷_贷后+贷前
            'info_com_bus_court_open_tax_arrears': None,  # 工商法院_在营企业_欠税名单_贷后+贷前
            'info_com_bus_court_open_pub_info': None,  # 工商法院_在营企业_执行公开信息_贷后+贷前
            'info_com_bus_court_open_admi_violation': None,  # 工商法院_在营企业_行政违法记录_贷后+贷前
            'info_com_bus_court_open_judge_docu': None,  # 工商法院_在营企业_民商事裁判文书_贷后+贷前
            'info_com_bus_court_open_judge_proc': None,  # 工商法院_在营企业_民商事审判流程_贷后+贷前
            'info_com_bus_court_open_tax_pay': None  # 工商法院_在营企业_纳税非正常户_贷后+贷前
        }
        self.pre_biz_date = None
        self.user_name = None
        self.id_card_no = None
        self.ent_dict = dict()

    # 获取企业对外投资企业中占股比例大于20%且在营的企业
    def _get_ent_list(self):
        sql1 = """
            select 
                distinct a.ent_name,a.credit_code
            from
                info_com_bus_entinvitem a
            left join
                (select id FROM info_com_bus_basic where create_time < NOW()
                and (ent_name=%(user_name)s or credit_code=%(id_card_no)s)
                order by create_time desc limit 1) b 
            on 
                a.basic_id=b.id
            where
                a.funded_ratio >= 0.2 and 
                (a.ent_status='在营（开业）' or a.ent_status='存续（在营、开业、在册）')
        """
        sql2 = """
            select 
                distinct a.ent_name,a.credit_code
            from
                info_com_bus_frinv a
            left join
                (select id FROM info_com_bus_basic where create_time < NOW()
                and (ent_name=%(user_name)s or credit_code=%(id_card_no)s)
                order by create_time desc limit 1) b 
            on 
                a.basic_id=b.id
            where
                a.funded_ratio >= 0.2 and 
                (a.ent_status='在营（开业）' or a.ent_status='存续（在营、开业、在册）')
        """
        df1 = sql_to_df(sql=sql1,
                        params={'user_name': self.user_name,
                                'id_card_no': self.id_card_no})
        df2 = sql_to_df(sql=sql2,
                        params={'user_name': self.user_name,
                                'id_card_no': self.id_card_no})
        df = pd.concat([df1, df2], axis=0, ignore_index=True, sort=False)
        if len(df) > 0:
            for row in df.itertuples():
                key = getattr(row, 'ent_name')
                if not self.ent_dict.__contains__(key):
                    self.ent_dict[key] = getattr(row, 'credit_code')
        return

    # 企业对外投资企业命中各类名单详细信息展示
    def _hit_list_details(self):
        hit_list = {
            'info_com_bus_court_open_owed_owe': 'info_court_arrearage',  # 工商法院_在营企业_欠款欠费名单_贷后+贷前
            'info_com_bus_court_open_court_dishonesty': 'info_court_deadbeat',  # 工商法院_在营企业_失信老赖名单_贷后+贷前
            'info_com_bus_court_open_rest_entry': 'info_court_limited_entry_exit',  # 工商法院_在营企业_限制出入境名单_贷后+贷前
            'info_com_bus_court_open_high_cons': 'info_court_limit_hignspending',  # 工商法院_在营企业_限制高消费名单_贷后+贷前
            'info_com_bus_court_open_cri_sus': 'info_court_criminal_suspect',  # 工商法院_在营企业_罪犯及嫌疑人名单_贷后+贷前
            'info_com_bus_court_open_tax_arrears': 'info_court_tax_arrears',  # 工商法院_在营企业_欠税名单_贷后+贷前
            'info_com_bus_court_open_pub_info': 'info_court_excute_public',  # 工商法院_在营企业_执行公开信息_贷后+贷前
            'info_com_bus_court_open_admi_violation': 'info_court_administrative_violation',  # 工商法院_在营企业_行政违法记录_贷后+贷前
            'info_com_bus_court_open_judge_docu': 'info_court_judicative_pape',  # 工商法院_在营企业_民商事裁判文书_贷后+贷前
            'info_com_bus_court_open_judge_proc': 'info_court_trial_process',  # 工商法院_在营企业_民商事审判流程_贷后+贷前
            'info_com_bus_court_open_tax_pay': 'info_court_taxable_abnormal_user'  # 工商法院_在营企业_纳税非正常户_贷后+贷前
        }
        for var in hit_list.keys():
            df_before_loan = pd.DataFrame()
            df_after_loan = pd.DataFrame()
            for ent in self.ent_dict.keys():
                sql_before_loan = """
                    select 
                        a.*
                    from 
                       %s a""" % hit_list[var] + """
                    left join 
                        (select id FROM info_court where %(result_date)s between create_time and expired_at
                        and (unique_name=%(user_name)s or unique_id_no=%(id_card_no)s) 
                        order by create_time desc limit 1) b 
                    on 
                        a.court_id=b.id
                    """
                sql_after_loan = """
                    select 
                        a.*
                    from 
                        %s a""" % hit_list[var] + """
                    left join 
                        (select id FROM info_court where NOW() between create_time and expired_at
                        and (unique_name=%(user_name)s or unique_id_no=%(id_card_no)s) 
                        order by create_time desc limit 1) b 
                    on 
                        a.court_id=b.id
                    """
                df_before_loan_temp = sql_to_df(sql=sql_before_loan,
                                                params={'result_date': self.pre_biz_date,
                                                        'user_name': ent,
                                                        'id_card_no': self.ent_dict[ent]})
                df_after_loan_temp = sql_to_df(sql=sql_after_loan,
                                               params={'user_name': ent,
                                                       'id_card_no': self.ent_dict[ent]})
                df_before_loan = pd.concat([df_before_loan, df_before_loan_temp], axis=0, ignore_index=True, sort=False)
                df_after_loan = pd.concat([df_after_loan, df_after_loan_temp], axis=0, ignore_index=True, sort=False)
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

    # 企业对外投资企业金融借款合同纠纷和民间借贷纠纷详细信息展示
    def _hit_contract_dispute_details(self):
        hit_list = {
            'info_com_bus_court_open_fin_loan_con': '金融借款合同纠纷',  # 工商法院_在营企业_金融借款合同纠纷_贷后+贷前
            'info_com_bus_court_open_pop_loan': '民间借贷纠纷'  # 工商法院_在营企业_民间借贷纠纷_贷后+贷前
        }
        for var in hit_list.keys():
            df_before_loan1 = pd.DataFrame()
            df_after_loan1 = pd.DataFrame()
            df_before_loan2 = pd.DataFrame()
            df_after_loan2 = pd.DataFrame()
            for ent in self.ent_dict.keys():
                sql_before_loan1 = """
                    select 
                        a.*
                    from 
                        info_court_judicative_pape a 
                    left join 
                        (select id FROM info_court where %(result_date)s between create_time and expired_at
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
                        (select id FROM info_court where NOW() between create_time and expired_at
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
                        (select id FROM info_court where %(result_date)s between create_time and expired_at
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
                        (select id FROM info_court where NOW() between create_time and expired_at
                        and (unique_name=%(user_name)s or unique_id_no=%(id_card_no)s)
                        order by create_time desc limit 1) b 
                    on 
                        a.court_id=b.id
                    where
                        a.case_reason like %(case_reason)s and a.legal_status like '%%被告%%'
                    """
                df_before_temp1 = sql_to_df(sql=sql_before_loan1,
                                            params={'result_date': self.pre_biz_date,
                                                    'user_name': ent,
                                                    'id_card_no': self.ent_dict[ent],
                                                    'case_reason': '%'+hit_list[var]+'%'})
                df_after_temp1 = sql_to_df(sql=sql_after_loan1,
                                           params={'user_name': ent,
                                                   'id_card_no': self.ent_dict[ent],
                                                   'case_reason': '%'+hit_list[var]+'%'})
                df_before_temp2 = sql_to_df(sql=sql_before_loan2,
                                            params={'result_date': self.pre_biz_date,
                                                    'user_name': ent,
                                                    'id_card_no': self.ent_dict[ent],
                                                    'case_reason': '%'+hit_list[var]+'%'})
                df_after_temp2 = sql_to_df(sql=sql_after_loan2,
                                           params={'user_name': ent,
                                                   'id_card_no': self.ent_dict[ent],
                                                   'case_reason': '%'+hit_list[var]+'%'})
                df_before_loan1 = pd.concat([df_before_loan1, df_before_temp1],
                                            axis=0, ignore_index=True, sort=False)
                df_after_loan1 = pd.concat([df_after_loan1, df_after_temp1],
                                           axis=0, ignore_index=True, sort=False)
                df_before_loan2 = pd.concat([df_before_loan2, df_before_temp2],
                                            axis=0, ignore_index=True, sort=False)
                df_after_loan2 = pd.concat([df_after_loan2, df_after_temp2],
                                           axis=0, ignore_index=True, sort=False)
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

    # 企业对外投资企业借款合同纠纷详细信息展示
    def _info_com_bus_court_open_loan_con(self):
        df_before_loan1 = pd.DataFrame()
        df_after_loan1 = pd.DataFrame()
        df_before_loan2 = pd.DataFrame()
        df_after_loan2 = pd.DataFrame()
        for ent in self.ent_dict.keys():
            sql_before_loan1 = """
                select 
                    a.*
                from 
                    info_court_judicative_pape a 
                left join 
                    (select id FROM info_court where %(result_date)s between create_time and expired_at
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
                    (select id FROM info_court where NOW() between create_time and expired_at
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
                    (select id FROM info_court where %(result_date)s between create_time and expired_at
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
                    (select id FROM info_court where NOW() between create_time and expired_at
                    and (unique_name=%(user_name)s or unique_id_no=%(id_card_no)s)
                    order by create_time desc limit 1) b 
                on 
                    a.court_id=b.id
                where
                    a.case_reason regexp '借款合同纠纷|民间借贷纠纷|金融不良债权追偿纠纷|
                                金融不良债权转让合同纠纷|企业借贷纠纷|同业拆借纠纷'
                    and a.legal_status like '%%被告%%'
                """
            df_before_temp1 = sql_to_df(sql=sql_before_loan1,
                                        params={'result_date': self.pre_biz_date,
                                                'user_name': ent,
                                                'id_card_no': self.ent_dict[ent]})
            df_after_temp1 = sql_to_df(sql=sql_after_loan1,
                                       params={'user_name': ent,
                                               'id_card_no': self.ent_dict[ent]})
            df_before_temp2 = sql_to_df(sql=sql_before_loan2,
                                        params={'result_date': self.pre_biz_date,
                                                'user_name': ent,
                                                'id_card_no': self.ent_dict[ent]})
            df_after_temp2 = sql_to_df(sql=sql_after_loan2,
                                       params={'user_name': ent,
                                               'id_card_no': self.ent_dict[ent]})
            df_before_loan1 = pd.concat([df_before_loan1, df_before_temp1],
                                        axis=0, ignore_index=True, sort=False)
            df_after_loan1 = pd.concat([df_after_loan1, df_after_temp1],
                                       axis=0, ignore_index=True, sort=False)
            df_before_loan2 = pd.concat([df_before_loan2, df_before_temp2],
                                        axis=0, ignore_index=True, sort=False)
            df_after_loan2 = pd.concat([df_after_loan2, df_after_temp2],
                                       axis=0, ignore_index=True, sort=False)
        self.variables['info_com_bus_court_open_loan_con'] = {'before': [], 'after': []}
        if len(df_before_loan1) > 0:
            for row in df_before_loan1.itertuples():
                self.variables['info_com_bus_court_open_loan_con']['before'].append({})
                for col in df_before_loan1.columns:
                    self.variables['info_com_bus_court_open_loan_con']['before'][-1][col] = getattr(row, col)
        if len(df_after_loan1) > 0:
            for row in df_after_loan1.itertuples():
                self.variables['info_com_bus_court_open_loan_con']['after'].append({})
                for col in df_after_loan1.columns:
                    self.variables['info_com_bus_court_open_loan_con']['after'][-1][col] = getattr(row, col)
        if len(df_before_loan2) > 0:
            for row in df_before_loan2.itertuples():
                self.variables['info_com_bus_court_open_loan_con']['before'].append({})
                for col in df_before_loan2.columns:
                    self.variables['info_com_bus_court_open_loan_con']['before'][-1][col] = getattr(row, col)
        if len(df_after_loan2) > 0:
            for row in df_after_loan2.itertuples():
                self.variables['info_com_bus_court_open_loan_con']['after'].append({})
                for col in df_after_loan2.columns:
                    self.variables['info_com_bus_court_open_loan_con']['after'][-1][col] = getattr(row, col)
        return

    # 执行变量转换
    def transform(self):
        self.pre_biz_date = self.origin_data.get('preBizDate')

        self.variables["variable_product_code"] = "06001"

        self._get_ent_list()
        if len(self.ent_dict) > 0:
            self._hit_list_details()
            self._hit_contract_dispute_details()
            self._info_com_bus_court_open_loan_con()
