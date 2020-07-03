from view.TransFlow import TransFlow



class single_loan_portrait(TransFlow):

    def read_single_loan_pt_process(self):

        df = self.cached_data['trans_single_loan_portrait']
        df.drop(columns = ['id','account_id','report_req_no','create_time','update_time'],
                    inplace = True)


        loan_type_list = ['消金','银行','融资租赁','担保','保理','小贷','其他金融','民间借贷']

        json =[]
        for loan in loan_type_list:
            temp_df = df[df.loan_type == loan]

            json.append("\"" + loan + "\":" +
                        temp_df.set_index('loan_type').to_json(orient='records').encode('utf-8').decode("unicode_escape")
                        + ",")

        string = ''
        for text in json:
            string += text

        self.variables['trans_single_loan_portrait'] = "{" + string[:-1] + "}"

from view.p08001_v.trans_flow import transform_class_str, months_ago
import pandas as pd


class SingleLoanPortrait:

    def __init__(self, trans_flow):
        self.trans_flow_portrait_df = trans_flow.trans_flow_portrait_df_2_year
        self.db = trans_flow.db
        self.variables = []

    # todo account_id
    def process(self):
        self._loan_type_detail()

        self.db.session.add_all(self.variables)
        self.db.session.commit()

    def _loan_type_detail(self):
        flow_df = self.trans_flow_portrait_df
        max_date = max(flow_df['trans_date'])
        min_date = min(flow_df['trans_date'])
        min_year = min_date.year
        min_month = min_date.month - 1
        flow_df['calendar_month'] = flow_df['trans_date'].apply(lambda x:
                                                                (x.year - min_year) * 12 + x.month - min_month)

        loan_type_list = list(set(flow_df['loan_type'].to_list()))
        months_cnt = flow_df['calendar_month'].max()

        for t in loan_type_list:
            # 24个公历月
            loan_type_df = flow_df[flow_df['loan_type'] == t]
            for i in range(1, months_cnt+1):
                temp_df = loan_type_df[loan_type_df['calendar_month'] == i]
                if len(temp_df) == 0:
                    continue
                temp_dict = dict()
                temp_dict['loan_type'] = t
                temp_dict['month'] = str(i)
                temp_dict['loan_amt'] = temp_df[temp_df['trans_amt'] > 0]['trans_amt'].sum()
                temp_dict['repay_amt'] = temp_df[temp_df['trans_amt'] < 0]['trans_amt'].sum()
                role = transform_class_str(temp_dict, 'TransSingleLoanPortrait')
                self.variables.append(role)
            # 近3/6/12个月及历史可查
            for j in [3, 6, 12, 24]:
                if j != 24:
                    temp_min_date = months_ago(max_date, j)
                    temp_df = loan_type_df[loan_type_df['trans_date'] >= temp_min_date]
                else:
                    temp_df = loan_type_df.copy()
                if len(temp_df) == 0:
                    continue
                temp_dict = dict()
                temp_income_df = temp_df[temp_df['trans_amt'] > 0]
                temp_expense_df = temp_df[temp_df['trans_amt'] < 0]
                temp_dict['loan_type'] = t
                temp_dict['month'] = '近' + str(j) + '个月' if j != 24 else '历史可查'
                temp_dict['loan_amt'] = temp_income_df['trans_amt'].sum()
                temp_dict['loan_cnt'] = temp_income_df.shape[0]
                temp_dict['loan_mean'] = temp_income_df['trans_amt'].mean()
                temp_dict['repay_amt'] = temp_expense_df['trans_amt'].sum()
                temp_dict['repay_cnt'] = temp_expense_df.shape[0]
                temp_dict['repay_mean'] = temp_expense_df['trans_amt'].mean()
                role = transform_class_str(temp_dict, 'TransSingleLoanPortrait')
                self.variables.append(role)



