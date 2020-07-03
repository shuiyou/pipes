from view.TransFlow import TransFlow
import pandas as pd


class single_remark_detail(TransFlow):

    def read_single_remark_flow_process(self):

        def connet_json( json ):
            string = ''
            for text in json :
                string += text
            return string[:-1]

        flow_df = self.cached_data['trans_flow_portrait'][['trans_date','trans_time','opponent_name','trans_amt','remark']]
        flow_df['trans_time'] = flow_df.apply(lambda x: pd.datetime.combine(x['trans_date'], x['trans_time']), 1)
        flow_df.drop(columns='trans_date', inplace=True)

        remark_portrait = self.cached_data['trans_single_remark_portrait']
        remark_portrait.drop(columns=['id','account_id','report_req_no','create_time','update_time'],
                             inplace = True)

        remark_income_dict = remark_portrait[pd.notnull(remark_portrait.remark_income_amt_order)] \
            [['remark_income_amt_order', 'remark_type']]. \
            set_index('remark_income_amt_order')['remark_type'].to_dict()

        json1 = []
        for i in remark_income_dict:
            # order列应为int
            # i = int(i)
            temp_df = flow_df[(flow_df.remark == remark_income_dict[i]) & (flow_df.trans_amt > 0)]. \
                rename(columns={'opponent_name': 'oppo_name'})
            json1.append((f"\"{i}\"" + ":" + temp_df.to_json(orient='records')) + ",")

        json_1 = connet_json(json1).encode('utf-8').decode("unicode_escape")


        remark_expense_dict = remark_portrait[pd.notnull(remark_portrait.remark_expense_amt_order)] \
            [['remark_expense_amt_order', 'remark_type']]. \
            set_index('remark_expense_amt_order')['remark_type'].to_dict()

        json2 = []
        for j in remark_expense_dict:
            # order列应为int
            # j = int(j)
            temp_df = flow_df[(flow_df.remark == remark_expense_dict[j]) & (flow_df.trans_amt < 0)]. \
                rename(columns={'opponent_name': 'oppo_name'})
            json2.append((f"\"{j}\"" + ":" + temp_df.to_json(orient='records')) + ",")

        json_2 = connet_json(json2).encode('utf-8').decode("unicode_escape")


        self.variables['异常交易风险'] = "{\"remark_income_amt_order\":{"+ json_1 + \
                                   "},\"remark_expense_amt_order\":{" + json_2 + "}}"