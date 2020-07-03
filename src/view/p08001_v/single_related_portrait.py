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