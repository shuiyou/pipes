
from portrait.transflow.single_account_portrait.trans_flow import transform_class_str, months_ago
import pandas as pd
import datetime


class SingleSummaryPortrait:
    """
    单账户画像表_按时间统计的汇总信息
    author:汪腾飞
    created_time:20200708
    updated_time_v1:
    """

    def __init__(self, trans_flow):
        self.trans_flow_portrait_df = trans_flow.trans_flow_portrait_df
        self.report_req_no = trans_flow.report_req_no
        self.account_id = trans_flow.account_id
        self.db = trans_flow.db
        self.role_list = []

    def process(self):
        if self.trans_flow_portrait_df is None:
            return
        self._calendar_month_detail()

        self.db.session.add_all(self.role_list)
        self.db.session.commit()

    def _calendar_month_detail(self):
        flow_df = self.trans_flow_portrait_df
        create_time = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')

        max_date = flow_df['trans_date'].max()
        min_date = flow_df['trans_date'].min()
        min_year = min_date.year
        min_month = min_date.month - 1
        flow_df['calendar_month'] = flow_df['trans_date'].apply(lambda x:
                                                                (x.year - min_year) * 12 + x.month - min_month)
        flow_df['month'] = flow_df['trans_date'].apply(lambda x: x.month)
        half_year_ago = months_ago(max_date, 6)
        year_ago = months_ago(max_date, 12)

        not_sensitive_df = flow_df[(pd.isnull(flow_df.relationship)) | (flow_df.is_sensitive == 1)]

        # 十三个公历月
        for i in range(13):
            temp_dict = {}
            normal_income_amt = not_sensitive_df[(not_sensitive_df.calendar_month == i) &
                                                 (not_sensitive_df.trans_amt > 0)]['trans_amt'].sum()
            normal_expense_amt = not_sensitive_df[(not_sensitive_df.calendar_month == i) &
                                                  (not_sensitive_df.trans_amt < 0)]['trans_amt'].sum()
            temp_df = flow_df[flow_df['calendar_month'] == i + 1]
            temp_dict['account_id'] = self.account_id
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

            role = transform_class_str(temp_dict, 'TransSingleSummaryPortrait')
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
            if i <= 4:
                interest_amt = flow_df[(flow_df.month == i * 3) &
                                       (flow_df.is_interest == 1)]['trans_amt'].fillna(0).sum()
            elif i == 5:
                interest_amt = flow_df[(flow_df.trans_date >= half_year_ago) &
                                       (flow_df.is_interest == 1)]['trans_amt'].fillna(0).mean()
            else:
                interest_amt = flow_df[(flow_df.trans_date > year_ago) &
                                       (flow_df.is_interest == 1)]['trans_amt'].fillna(0).mean()
            interest_amt = interest_amt * 4 / 0.003 if pd.notna(interest_amt) else 0
            if i <= 4:
                end_date_list = flow_df[(flow_df.month == i * 3) &
                                        (flow_df.is_interest == 1)]['trans_date'].to_list()
                if len(end_date_list) == 0:
                    continue
                end_date = end_date_list[0]
            else:
                end_date = max_date
            if i == 1:
                start_date = datetime.datetime(end_date.year - 1, 12, end_date.day + 1)
            elif i <= 4:
                start_date = datetime.datetime(end_date.year, end_date.month - 3, end_date.day + 1)
            elif i == 5:
                start_date = half_year_ago
            else:
                start_date = year_ago
            balance_df = flow_df[(flow_df.trans_date >= start_date) & (flow_df.trans_date <= end_date)]
            balance_df.sort_values(by=['trans_date', 'trans_time'], ascending=True, inplace=True)
            balance_df['str_date'] = balance_df['trans_date'].apply(lambda x:
                                                                    x.date if type(x) == datetime.datetime else x)
            balance_df.drop_duplicates(subset='str_date', keep='last', inplace=True)
            str_date = balance_df['trans_date'].to_list()
            trans_amt = balance_df['trans_amt'].to_list()
            length = len(str_date)
            diff_days = [(str_date[i+1] - str_date[i]).days for i in range(length - 1)]
            diff_days.append(1)
            total_days = sum(diff_days)
            total_amt = [diff_days[i] * trans_amt[i] for i in range(length)]
            balance_amt = sum(total_amt) / total_days if total_days != 0 else 0
            interest_balance_proportion = interest_amt / balance_amt if balance_amt != 0 else 0

            temp_dict1['account_id'] = self.account_id
            temp_dict1['report_req_no'] = self.report_req_no
            temp_dict1['month'] = value
            temp_dict1['interest_amt'] = interest_amt
            temp_dict1['balance_amt'] = balance_amt
            temp_dict1['interest_balance_proportion'] = interest_balance_proportion
            temp_dict1['create_time'] = create_time
            temp_dict1['update_time'] = create_time

            role1 = transform_class_str(temp_dict1, 'TransSingleSummaryPortrait')
            self.role_list.append(role1)
