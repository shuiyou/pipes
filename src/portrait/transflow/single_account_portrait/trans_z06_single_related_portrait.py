
from portrait.transflow.single_account_portrait.trans_flow import transform_class_str
import pandas as pd
import datetime


class SingleRelatedPortrait:
    """
    单账户画像表_关联人和担保人流水信息
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
        self._relationship_detail()

        self.db.session.add_all(self.role_list)
        self.db.session.commit()

    def _relationship_detail(self):
        flow_df = self.trans_flow_portrait_df
        create_time = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')

        flow_df = flow_df[(pd.notnull(flow_df.relationship)) &
                          (pd.notnull(flow_df.opponent_name))]
        total_income = flow_df[flow_df.trans_amt > 0]['trans_amt'].sum()
        total_expense = flow_df[flow_df.trans_amt < 0]['trans_amt'].sum()

        base_type = list(set(flow_df['relationship'].to_list()))
        for t in base_type:
            temp_t_df = flow_df[flow_df.relationship == t]
            name_list = list(set(temp_t_df['opponent_name'].to_list()))
            if len(name_list) == 0:
                continue
            for n in name_list:
                temp_df = temp_t_df[temp_t_df.opponent_name == n]
                temp_dict = dict()
                temp_dict['account_id'] = self.account_id
                temp_dict['report_req_no'] = self.report_req_no
                temp_dict['opponent_name'] = n
                temp_dict['relationship'] = t
                temp_dict['income_cnt'] = temp_df[temp_df['trans_amt'] > 0].shape[0]
                income_amt = temp_df[temp_df['trans_amt'] > 0]['trans_amt'].sum()
                temp_dict['income_amt'] = income_amt
                temp_dict['income_amt_proportion'] = income_amt / total_income if total_income != 0 else 0

                temp_dict['expense_cnt'] = temp_df[temp_df['trans_amt'] < 0].shape[0]
                expense_amt = temp_df[temp_df['trans_amt'] < 0]['trans_amt'].sum()
                temp_dict['expense_amt'] = expense_amt
                temp_dict['expense_amt_proportion'] = expense_amt / total_expense if total_expense != 0 else 0
                temp_dict['create_time'] = create_time
                temp_dict['update_time'] = create_time

                role = transform_class_str(temp_dict, 'TransSingleRelatedPortrait')
                self.role_list.append(role)
