from view.TransFlow import TransFlow

class JsonSingleSummaryPortrait(TransFlow):

    def process(self):
        self.read_single_summary_pt()

    def read_single_summary_pt(self):
        df = self.cached_data['trans_single_summary_portrait']
        df.drop(columns=['id', 'account_id', 'report_req_no', 'q_1_year', 'q_2_year',
                         'q_3_year', 'q_4_year', 'create_time', 'update_time'],
                inplace=True)

        # col_list = df.columns.tolist()
        # month_list = list(map(str, list(range(1,14)))) + \
        #              ['quarter1','quarter2','quarter3','quarter4','half_year','year']
        #
        # for m in month_list:
        #     temp_df = df[df.month == m]
        #     if temp_df.empty:
        self.variables["trans_single_summary_portrait"] = df.to_json(orient='records')
