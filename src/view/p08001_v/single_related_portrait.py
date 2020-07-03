from view.TransFlow import TransFlow

class trans_single_related_portrait(TransFlow):

    def read_single_related_pt_process(self):

        df = self.cached_data['trans_single_related_portrait']
        df.drop(columns = ['id','account_id','report_req_no',
                           'income_cnt_order','income_amt_order',
                           'expense_cnt_order','expense_amt_order',
                           'create_time','update_time'],
                inplace = True)

        self.variables['trans_single_related_portrait'] = df.to_json(orient='records').encode('utf-8').decode("unicode_escape")

from view.p08001_v.trans_flow import transform_class_str
import pandas as pd


class SingleRelatedPortrait:

    def __init__(self, trans_flow):
        self.trans_flow_portrait_df = trans_flow.trans_flow_portrait_df
        self.db = trans_flow.db
        self.variables = []

    # todo account_id
    def process(self):
        self._relationship_detail()

        self.db.session.add_all(self.variables)
        self.db.session.commit()

    def _relationship_detail(self):
        flow_df = self.trans_flow_portrait_df
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

                role = transform_class_str(temp_dict, 'TransSingleRelatedPortrait')
                self.variables.append(role)
