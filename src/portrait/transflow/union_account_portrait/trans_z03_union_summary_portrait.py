
from portrait.transflow.single_account_portrait.trans_flow import transform_class_str, months_ago
from util.mysql_reader import sql_to_df
import pandas as pd
import datetime


class UnionSummaryPortrait:
    """
    联合账户画像表_按时间统计的汇总信息
    author:汪腾飞
    created_time:20200708
    updated_time_v1:
    """

    def __init__(self, trans_flow):
        self.trans_flow_portrait_df = trans_flow.trans_u_flow_portrait_df
        self.report_req_no = trans_flow.report_req_no
        self.app_no = trans_flow.app_no
        self.db = trans_flow.db
        self.role_list = []

    def process(self):
        if self.trans_flow_portrait_df is None:
            return
        self._calendar_month_detail()

        self.db.session.add_all(self.role_list)
        self.db.session.commit()

    def _single_portrait(self):
        sql = """select * from trans_single_summary_portrait where report_req_no = '%s'""" % self.report_req_no
        single_u_df = sql_to_df(sql)
        return single_u_df

    def _calendar_month_detail(self):
        flow_df = self.trans_flow_portrait_df
        single_u_df = self._single_portrait()
        create_time = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')

        # max_date = flow_df['trans_date'].max()
        min_date = flow_df['trans_date'].min()
        min_year = min_date.year
        min_month = min_date.month - 1
        flow_df['calendar_month'] = flow_df['trans_date'].apply(lambda x:
                                                                (x.year - min_year) * 12 + x.month - min_month)
        flow_df['month'] = flow_df['trans_date'].apply(lambda x: x.month)

        not_sensitive_df = flow_df[(pd.isnull(flow_df.relationship)) & (flow_df.is_sensitive != 1)]
        cost_df = flow_df[pd.notnull(flow_df.cost_type)]

        # 十三个公历月
        for i in range(13):
            temp_dict = {}
            normal_income_amt = not_sensitive_df[(not_sensitive_df.calendar_month == i + 1) &
                                                 (not_sensitive_df.trans_amt > 0)]['trans_amt'].sum()
            normal_expense_amt = cost_df[cost_df.calendar_month == i + 1]['trans_amt'].sum()
            temp_df = flow_df[flow_df['calendar_month'] == i + 1]
            temp_dict['apply_no'] = self.app_no
            temp_dict['report_req_no'] = self.report_req_no
            temp_dict['month'] = str(i+1)
            temp_dict['normal_income_amt'] = normal_income_amt
            temp_dict['normal_expense_amt'] = normal_expense_amt
            temp_dict['net_income_amt'] = normal_income_amt + normal_expense_amt
            temp_dict['salary_cost_amt'] = temp_df[temp_df.cost_type == '工资']['trans_amt'].sum()
            temp_dict['living_cost_amt'] = temp_df[temp_df.cost_type == '水电']['trans_amt'].sum()
            temp_dict['tax_cost_amt'] = temp_df[temp_df.cost_type == '税费']['trans_amt'].sum()
            temp_dict['rent_cost_amt'] = temp_df[temp_df.cost_type == '房租']['trans_amt'].sum()
            temp_dict['insurance_cost_amt'] = temp_df[temp_df.cost_type == '保险']['trans_amt'].sum()
            temp_dict['loan_cost_amt'] = temp_df[temp_df.cost_type == '到期贷款']['trans_amt'].sum()
            temp_dict['create_time'] = create_time
            temp_dict['update_time'] = create_time

            role = transform_class_str(temp_dict, 'TransUSummaryPortrait')
            self.role_list.append(role)

        # 四个季度和半年以及整年
        for i in range(1, 7):
            if i <= 4:
                value = 'quarter' + str(i)
            elif i == 5:
                value = 'half_year'
            else:
                value = 'year'
            temp_dict1 = {}

            interest_amt = single_u_df[single_u_df['month'] == value]['interest_amt'].sum()
            balance_amt = single_u_df[single_u_df['month'] == value]['balance_amt'].sum()
            interest_balance_proportion = interest_amt / balance_amt if balance_amt != 0 else 0

            temp_dict1['apply_no'] = self.app_no
            temp_dict1['report_req_no'] = self.report_req_no
            temp_dict1['month'] = value
            temp_dict1['interest_amt'] = interest_amt
            temp_dict1['balance_amt'] = balance_amt
            temp_dict1['interest_balance_proportion'] = interest_balance_proportion
            temp_dict1['create_time'] = create_time
            temp_dict1['update_time'] = create_time

            role1 = transform_class_str(temp_dict1, 'TransUSummaryPortrait')
            self.role_list.append(role1)
