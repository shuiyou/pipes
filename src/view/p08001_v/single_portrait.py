
from view.p08001_v.trans_flow import transform_class_str
import pandas as pd
import datetime


class SingleProtrait:

    def __init__(self, trans_flow):
        self.trans_flow_portrait_df = trans_flow.trans_flow_portrait_df
        self.db = trans_flow.db
        self.variables = {}

    def process(self):
        self._analyse_time()
        self._trans_amt()
        self._relationship_risk()
        self._income_detail()

        role = transform_class_str(self.variables, 'TransSinglePortrait')
        self.db.session.add(role)
        self.db.session.commit()

    # todo account_id
    def _analyse_time(self):
        flow_df = self.trans_flow_portrait_df
        max_date = max(flow_df['trans_date'])
        min_date = min(flow_df['trans_date'])

        not_full_month = []
        if min_date.day != 1:
            not_full_month.append(datetime.datetime.strftime(min_date, '%Y-%m'))

        if (max_date + datetime.timedelta(days=1)).day != 1:
            not_full_month.append(datetime.datetime.strftime(max_date, '%Y-%m'))
        self.variables['analyse_start_time'] = datetime.datetime.strftime(min_date, '%Y-%m-%d')
        self.variables['analyse_end_time'] = datetime.datetime.strftime(max_date, '%Y-%m-%d')
        self.variables['not_full_month'] = ','.join(not_full_month)

    def _trans_amt(self):
        flow_df = self.trans_flow_portrait_df
        df = flow_df[((pd.isnull(flow_df.relationship)) | (flow_df.is_sensitive == 1)) &
                     (flow_df.trans_amt > 0)]
        df['trans_year'] = df['trans_date'].apply(lambda x: x.year)
        df['trans_month'] = df['trans_date'].apply(lambda x: x.month)
        self.variables['normal_income_amt'] = df.trans_amt.sum()
        self.variables['normal_income_cnt'] = df.shape[0]
        self.variables['normal_income_mean'] = df.trans_amt.mean()
        self.variables['normal_income_d_mean'] = df.groupby(by='trans_date').agg({'trans_amt': sum})['trans_amt'].mean()

        month_mean_income = df.groupby(by=['trans_year', 'trans_month']).\
            agg({'trans_amt': sum})['trans_amt'].mean()
        self.variables['normal_income_m_mean'] = month_mean_income
        self.variables['income_amt_y_pred'] = month_mean_income * 12
        self.variables['normal_income_m_mean'] = df.groupby(by=['trans_year', 'trans_month']).\
            agg({'trans_amt': sum})['trans_amt'].std()

        expense_df = flow_df[((pd.isnull(flow_df.relationship)) | (flow_df.is_sensitive == '1')) &
                             (flow_df.trans_amt < 0)]
        self.variables['normal_expense_amt'] = expense_df.trans_amt.sum()
        self.variables['normal_expense_cnt'] = expense_df.shape[0]

    def _relationship_risk(self):
        flow_df = self.trans_flow_portrait_df
        related_income_amt = flow_df[(flow_df.trans_amt > 0) & (pd.notnull(flow_df.relationship))]['trans_amt'].sum()
        unrelated_income_amt = flow_df[(flow_df.trans_amt > 0) & (pd.isnull(flow_df.relationship))]['trans_amt'].sum()
        self.variables['relationship_risk'] = 1 if unrelated_income_amt < related_income_amt else 0

    def _income_detail(self):
        flow_df = self.trans_flow_portrait_df
        income_list = [0, 5, 10, 30, 50, 100, 200]
        temp_df = flow_df.sort_values(by='trans_date', ascending=True)
        temp_df['str_date'] = temp_df['trans_date'].apply(lambda x: datetime.datetime.strftime(x, '%Y-%m-%d'))
        temp_df.drop_duplicates(subset='str_date', keep='last', inplace=True)
        income_weight_max = 0
        income_weight_min = 0
        balance_weight_max = 0
        balance_weight_min = 0
        for i in range(7):
            if i != 6:
                income_variable = 'income_' + str(income_list[i]) + '_to_' + str(income_list[i+1]) + '_cnt'
                balance_variable = 'balance_' + str(income_list[i]) + '_to_' + str(income_list[i+1]) + '_day'
                if i == 5:
                    income_variable = income_variable[1:]
                left = income_list[i] * 10000
                right = income_list[i+1] * 10000
                income_df = flow_df[(flow_df.trans_amt > left) & (flow_df.trans_amt <= right)]
                balance_df = temp_df[(temp_df.account_balance > left) & (temp_df.account_balance <= right)]
            else:
                income_variable = 'income_above_' + str(income_list[i]) + '_cnt'
                balance_variable = 'balance_above_' + str(income_list[i]) + '_day'
                left = income_list[i] * 10000
                income_df = flow_df[flow_df.trans_amt > left]
                balance_df = temp_df[temp_df.account_balance > left]
            temp_income_max = income_df['trans_amt'].max() if len(income_df) > 0 else 0
            temp_income_min = income_df['trans_amt'].min() if len(income_df) > 0 else 0
            temp_income_cnt = income_df.shape[0]
            income_weight_max += temp_income_max * temp_income_cnt
            income_weight_min += temp_income_min * temp_income_cnt

            temp_balance_max = balance_df['account_balance'].max() if len(balance_df) > 0 else 0
            temp_balance_min = balance_df['account_balance'].min() if len(balance_df) > 0 else 0
            temp_balance_cnt = balance_df.shape[0]
            balance_weight_max += temp_balance_max * temp_balance_cnt
            balance_weight_min += temp_balance_min * temp_balance_cnt
            self.variables[income_variable] = temp_income_cnt
            self.variables[balance_variable] = temp_balance_cnt
        self.variables['income_weight_max'] = income_weight_max
        self.variables['income_weight_min'] = income_weight_min
        self.variables['balance_weight_max'] = balance_weight_max
        self.variables['balance_weight_min'] = balance_weight_min
