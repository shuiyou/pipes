import pandas as pd
from mapping.tranformer import Transformer
from util.mysql_reader import sql_to_df
from mapping.utils.df_comparator_util import df_compare


class Tf0004(Transformer):
    """
    企业工商法院核查,贷后新增
    """
    def __init__(self):
        super().__init__()
        self.variables = {
            "com_bus_court_open_owed_owe_laf": 0,  # 工商法院_在营企业_欠款欠费名单命中次数_贷后新增
            "com_bus_court_open_court_dishonesty_laf": 0,  # 工商法院_在营企业_失信老赖名单命中次数_贷后新增
            "com_bus_court_open_rest_entry_laf": 0,  # 工商法院_在营企业_限制出入境名单命中次数_贷后新增
            "com_bus_court_open_high_cons_laf": 0,  # 工商法院_在营企业_限制高消费名单命中次数_贷后新增
            "com_bus_court_open_cri_sus_laf": 0,  # 工商法院_在营企业_罪犯及嫌疑人名单命中次数_贷后新增
            "com_bus_court_open_fin_loan_con_laf": 0,  # 工商法院_在营企业_金融借款合同纠纷_贷后新增
            "com_bus_court_open_loan_con_laf": 0,  # 工商法院_在营企业_借款合同纠纷_贷后新增
            "com_bus_court_open_pop_loan_laf": 0,  # 工商法院_在营企业_民间借贷纠纷_贷后新增
            "com_bus_court_open_tax_arrears_laf": 0,  # 工商法院_在营企业_欠税名单命中次数_贷后新增
            "com_bus_court_open_pub_info_laf": 0,  # 工商法院_在营企业_执行公开信息命中次数_贷后新增
            "com_bus_court_open_admi_violation_laf": 0,  # 工商法院_在营企业_行政违法记录命中次数_贷后新增
            "com_bus_court_open_judge_docu_laf": 0,  # 工商法院_在营企业_民商事裁判文书命中次数_贷后新增
            "com_bus_court_open_judge_proc_laf": 0,  # 工商法院_在营企业_民商事审判流程命中次数_贷后新增
            "com_bus_court_open_tax_pay_laf": 0  # 工商法院_在营企业_纳税非正常户命中次数_贷后新增
        }
        self.pre_biz_date = None
        self.ent_list = list()

    # 获取企业对外投资企业中占股比例大于20%且在营的企业
    def _get_ent_list(self):
        sql1 = """
            select 
                distinct a.ent_name,a.credit_code
            from
                info_com_bus_entinvitem a,
                (select id FROM info_com_bus_basic where create_time < NOW()
                and (ent_name=%(user_name)s or credit_code=%(id_card_no)s)
                order by id desc limit 1) b 
            where
                a.basic_id=b.id and
                a.funded_ratio >= 0.2 and 
                (a.ent_status='在营（开业）' or a.ent_status='存续（在营、开业、在册）')
        """
        sql2 = """
            select 
                distinct a.ent_name,a.credit_code
            from
                info_com_bus_frinv a,
                (select id FROM info_com_bus_basic where create_time < NOW()
                and (ent_name=%(user_name)s or credit_code=%(id_card_no)s)
                order by id desc limit 1) b 
            where
                a.basic_id=b.id and
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
            df.fillna('Na', inplace=True)
            df.replace('', 'Na', inplace=True)
            ent_str = '|'.join(list(df['ent_name']))
            credit_str = '|'.join(list(df['credit_code']))
            self.ent_list.append(ent_str)
            self.ent_list.append(credit_str)
        return

    # 判断各个变量命中次数是否有变化
    def _hit_list_times(self):
        hit_list = {
            'com_bus_court_open_owed_owe_laf': {'table_name': 'info_court_arrearage',
                                                'column_name': 'contract_no'},  # 工商法院_在营企业_欠款欠费名单命中次数_贷后新增
            'com_bus_court_open_court_dishonesty_laf': {'table_name': 'info_court_deadbeat',
                                                        'column_name': 'execute_case_no'},  # 工商法院_在营企业_失信老赖名单命中次数_贷后新增
            'com_bus_court_open_rest_entry_laf': {'table_name': 'info_court_limited_entry_exit',
                                                  'column_name': 'execute_no'},  # 工商法院_在营企业_限制出入境名单命中次数_贷后新增
            'com_bus_court_open_high_cons_laf': {'table_name': 'info_court_limit_hignspending',
                                                 'column_name': 'execute_case_no'},  # 工商法院_在营企业_限制高消费名单命中次数_贷后新增
            'com_bus_court_open_cri_sus_laf': {'table_name': 'info_court_criminal_suspect',
                                               'column_name': 'case_no'},  # 工商法院_在营企业_罪犯及嫌疑人名单命中次数_贷后新增
            'com_bus_court_open_judge_docu_laf': {'table_name': 'info_court_judicative_pape',
                                                  'column_name': 'case_no'},  # 工商法院_在营企业_民商事裁判文书命中次数_贷后新增
            'com_bus_court_open_judge_proc_laf': {'table_name': 'info_court_trial_process',
                                                  'column_name': 'case_no'},  # 工商法院_在营企业_民商事审判流程命中次数_贷后新增
            'com_bus_court_open_admi_violation_laf': {'table_name': 'info_court_administrative_violation',
                                                      'column_name': 'case_no'}  # 工商法院_在营企业_执行公开信息命中次数_贷后新增
        }
        for var in hit_list.keys():
            sql_before_loan = """
                select 
                    a.%s""" % hit_list[var]['column_name'] + """ as key_no
                from 
                    %s a""" % hit_list[var]['table_name'] + """,
                    (select max(id) as id FROM info_court where create_time < %(result_date)s
                    and (unique_name regexp %(user_name)s or unique_id_no regexp %(id_card_no)s) 
                    group by unique_name,unique_id_no) b 
                where
                    a.court_id=b.id
                """
            sql_after_loan = """
                select 
                    a.%s""" % hit_list[var]['column_name'] + """ as key_no
                from 
                    %s a""" % hit_list[var]['table_name'] + """,
                    (select max(id) as id FROM info_court where create_time < NOW()
                    and (unique_name regexp %(user_name)s or unique_id_no regexp %(id_card_no)s) 
                    group by unique_name,unique_id_no) b 
                where
                    a.court_id=b.id
                """
            df_before_loan = sql_to_df(sql=sql_before_loan,
                                       params={'result_date': self.pre_biz_date,
                                               'user_name': self.ent_list[0],
                                               'id_card_no': self.ent_list[1]})
            df_after_loan = sql_to_df(sql=sql_after_loan,
                                      params={'user_name': self.ent_list[0],
                                              'id_card_no': self.ent_list[1]})
            df_compare(self.variables, df_before_loan, df_after_loan, var)

    # 判断命中合同纠纷次数相关变量是否发生变化
    def _hit_contract_dispute_times(self):
        hit_list = {
            'com_bus_court_open_fin_loan_con_laf': '金融借款合同纠纷',  # 工商法院_在营企业_金融借款合同纠纷_贷后新增
            'com_bus_court_open_pop_loan_laf': '民间借贷纠纷'  # 工商法院_在营企业_民间借贷纠纷_贷后新增
        }
        for var in hit_list.keys():
            sql_before_loan1 = """
                select 
                    a.case_no as key_no
                from 
                    info_court_judicative_pape a,
                    (select max(id) as id FROM info_court where create_time < %(result_date)s
                    and (unique_name regexp %(user_name)s or unique_id_no regexp %(id_card_no)s) 
                    group by unique_name,unique_id_no) b 
                where
                    a.court_id=b.id and
                    a.case_reason like %(case_reason)s and a.legal_status like '%%被告%%'
                """
            sql_after_loan1 = """
                select 
                    a.case_no as key_no
                from 
                    info_court_judicative_pape a, 
                    (select max(id) as id FROM info_court where create_time < NOW()
                    and (unique_name regexp %(user_name)s or unique_id_no regexp %(id_card_no)s) 
                    group by unique_name,unique_id_no) b 
                where
                    a.court_id=b.id and
                    a.case_reason like %(case_reason)s and a.legal_status like '%%被告%%'
                """
            sql_before_loan2 = """
                select 
                    a.case_no as key_no
                from 
                    info_court_trial_process a,
                    (select max(id) as id FROM info_court where create_time < %(result_date)s
                    and (unique_name regexp %(user_name)s or unique_id_no regexp %(id_card_no)s) 
                    group by unique_name,unique_id_no) b 
                where
                    a.court_id=b.id and
                    a.case_reason like %(case_reason)s and a.legal_status like '%%被告%%'
                """
            sql_after_loan2 = """
                select 
                    a.case_no as key_no
                from 
                    info_court_trial_process a,
                    (select max(id) as id FROM info_court where create_time < NOW()
                    and (unique_name regexp %(user_name)s or unique_id_no regexp %(id_card_no)s) 
                    group by unique_name,unique_id_no) b 
                where
                    a.court_id=b.id and
                    a.case_reason like %(case_reason)s and a.legal_status like '%%被告%%'
                """
            df_before_loan1 = sql_to_df(sql=sql_before_loan1,
                                        params={'result_date': self.pre_biz_date,
                                                'user_name': self.ent_list[0],
                                                'id_card_no': self.ent_list[1],
                                                'case_reason': '%'+hit_list[var]+'%'})
            df_after_loan1 = sql_to_df(sql=sql_after_loan1,
                                       params={'user_name': self.ent_list[0],
                                               'id_card_no': self.ent_list[1],
                                               'case_reason': '%'+hit_list[var]+'%'})
            df_before_loan2 = sql_to_df(sql=sql_before_loan2,
                                        params={'result_date': self.pre_biz_date,
                                                'user_name': self.ent_list[0],
                                                'id_card_no': self.ent_list[1],
                                                'case_reason': '%'+hit_list[var]+'%'})
            df_after_loan2 = sql_to_df(sql=sql_after_loan2,
                                       params={'user_name': self.ent_list[0],
                                               'id_card_no': self.ent_list[1],
                                               'case_reason': '%'+hit_list[var]+'%'})
            df_before_loan = pd.concat([df_before_loan1, df_before_loan2],
                                       axis=0, ignore_index=True, sort=False)
            df_after_loan = pd.concat([df_after_loan1, df_after_loan2],
                                      axis=0, ignore_index=True, sort=False)
            df_compare(self.variables, df_before_loan, df_after_loan, var)

    # 判断借款合同纠纷命中数是否有新增
    def _com_bus_court_open_loan_con_laf(self):
        sql_before_loan1 = """
            select 
                a.case_no as key_no
            from 
                info_court_judicative_pape a,
                (select max(id) as id FROM info_court where create_time < %(result_date)s
                and (unique_name regexp %(user_name)s or unique_id_no regexp %(id_card_no)s) 
                group by unique_name,unique_id_no) b 
            where
                a.court_id=b.id and
                a.case_reason regexp '借款合同纠纷|民间借贷纠纷|金融不良债权追偿纠纷|
                                金融不良债权转让合同纠纷|企业借贷纠纷|同业拆借纠纷'
                and a.legal_status like '%%被告%%'
            """
        sql_after_loan1 = """
            select 
                a.case_no as key_no
            from 
                info_court_judicative_pape a,
                (select max(id) as id FROM info_court where create_time < NOW()
                and (unique_name regexp %(user_name)s or unique_id_no regexp %(id_card_no)s) 
                group by unique_name,unique_id_no) b 
            where
                a.court_id=b.id and
                a.case_reason regexp '借款合同纠纷|民间借贷纠纷|金融不良债权追偿纠纷|
                                金融不良债权转让合同纠纷|企业借贷纠纷|同业拆借纠纷'
                and a.legal_status like '%%被告%%'
            """
        sql_before_loan2 = """
            select 
                a.case_no as key_no
            from 
                info_court_trial_process a,
                (select max(id) as id FROM info_court where create_time < %(result_date)s
                and (unique_name regexp %(user_name)s or unique_id_no regexp %(id_card_no)s) 
                group by unique_name,unique_id_no) b 
            where
                a.court_id=b.id and
                a.case_reason regexp '借款合同纠纷|民间借贷纠纷|金融不良债权追偿纠纷|
                                金融不良债权转让合同纠纷|企业借贷纠纷|同业拆借纠纷'
                and a.legal_status like '%%被告%%'
            """
        sql_after_loan2 = """
            select 
                a.case_no as key_no
            from 
                info_court_trial_process a,
                (select max(id) as id FROM info_court where create_time < NOW()
                and (unique_name regexp %(user_name)s or unique_id_no regexp %(id_card_no)s) 
                group by unique_name,unique_id_no) b 
            where
                a.court_id=b.id and
                a.case_reason regexp '借款合同纠纷|民间借贷纠纷|金融不良债权追偿纠纷|
                                金融不良债权转让合同纠纷|企业借贷纠纷|同业拆借纠纷'
                and a.legal_status like '%%被告%%'
            """
        df_before_loan1 = sql_to_df(sql=sql_before_loan1,
                                    params={'result_date': self.pre_biz_date,
                                            'user_name': self.ent_list[0],
                                            'id_card_no': self.ent_list[1]})
        df_after_loan1 = sql_to_df(sql=sql_after_loan1,
                                   params={'user_name': self.ent_list[0],
                                           'id_card_no': self.ent_list[1]})
        df_before_loan2 = sql_to_df(sql=sql_before_loan2,
                                    params={'result_date': self.pre_biz_date,
                                            'user_name': self.ent_list[0],
                                            'id_card_no': self.ent_list[1]})
        df_after_loan2 = sql_to_df(sql=sql_after_loan2,
                                   params={'user_name': self.ent_list[0],
                                           'id_card_no': self.ent_list[1]})
        df_before_loan = pd.concat([df_before_loan1, df_before_loan2],
                                   axis=0, ignore_index=True, sort=False)
        df_after_loan = pd.concat([df_after_loan1, df_after_loan2],
                                  axis=0, ignore_index=True, sort=False)
        df_compare(self.variables, df_before_loan, df_after_loan, 'com_bus_court_open_loan_con_laf')

    # 统计欠税名单,纳税非正常户新增命中次数
    def _count_hit_times_newly_increased(self):
        hit_list = {
            'com_bus_court_open_tax_arrears_laf': {'table_name': 'info_court_tax_arrears',
                                                   'column_name': 'taxes_time'},  # 工商法院_在营企业_欠税名单命中次数_贷后新增
            'com_bus_court_open_tax_pay_laf': {'table_name': 'info_court_taxable_abnormal_user',
                                               'column_name': 'confirm_date'}  # 工商法院_在营企业_纳税非正常户命中次数_贷后新增
        }
        for var in hit_list.keys():
            sql = """
                select 
                    count(*) as cnt
                from
                    %s a""" % hit_list[var]['table_name'] + """,
                    (select max(id) as id FROM info_court where create_time < NOW()
                    and (unique_name regexp %(user_name)s or unique_id_no regexp %(id_card_no)s) 
                    group by unique_name,unique_id_no) b 
                where
                    a.court_id=b.id and
                     a.""" + hit_list[var]['column_name'] + """ between %(result_date)s and NOW()
            """
            df = sql_to_df(sql=sql,
                           params={'result_date': self.pre_biz_date,
                                   'user_name': self.ent_list[0],
                                   'id_card_no': self.ent_list[1]})
            self.variables[var] = df.values[0][0]

    # 统计执行公开信息新增命中次数
    def _com_bus_court_open_pub_info_laf(self):
        sql = """
            select 
                count(*) as cnt
            from
                info_court_excute_public a,
                (select max(id) as id FROM info_court where create_time < NOW()
                and (unique_name regexp %(user_name)s or unique_id_no regexp %(id_card_no)s) 
                group by unique_name,unique_id_no) b 
            where
                a.court_id=b.id
        """
        df = sql_to_df(sql=sql,
                       params={'user_name': self.ent_list[0],
                               'id_card_no': self.ent_list[1]})
        self.variables['com_bus_court_open_pub_info_laf'] = df.values[0][0]

    # 执行变量转换
    def transform(self):
        self.pre_biz_date = self.origin_data.get('preBizDate')
        if self.user_name is None or len(self.user_name) == 0:
            self.user_name = 'Na'
        if self.id_card_no is None or len(self.id_card_no) == 0:
            self.id_card_no = 'Na'

        self.variables["variable_product_code"] = "06001"

        self._get_ent_list()
        if len(self.ent_list) > 0:
            self._hit_list_times()
            self._hit_contract_dispute_times()
            self._com_bus_court_open_loan_con_laf()
            self._count_hit_times_newly_increased()
            self._com_bus_court_open_pub_info_laf()
