from view.TransFlow import TransFlow

class trans_u_summary_portrait(TransFlow):

    def read_u_summary_pt_process(self):

        df = self.cached_data['trans_u_summary_portrait']
        df.drop(columns=['id', 'apply_no', 'report_req_no', 'q_1_year', 'q_2_year',
                         'q_3_year', 'q_4_year', 'create_time', 'update_time'],
                inplace=True)

        self.variables['trans_u_summary_portrait'] = df.to_json(orient='records')