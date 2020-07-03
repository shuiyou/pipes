from view.TransFlow import TransFlow

class trans_u_portrait(TransFlow):

    def read_u_pt_process(self):

        df = self.cached_data['trans_u_portrait']
        df.drop(columns = ['id','apply_no','report_req_no','create_time','update_time'],inplace = True)

        self.variables['trans_u_portrait'] = df.to_json(orient='records')[1:-1]