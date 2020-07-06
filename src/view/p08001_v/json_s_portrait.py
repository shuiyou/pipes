from view.TransFlow import TransFlow

class JsonSinglePortrait(TransFlow):

    def process(self):
        self.read_single_pt()

    def read_single_pt(self):

        df = self.cached_data['trans_single_portrait']
        df.drop(columns = ['id','account_id','report_req_no','create_time','update_time'],inplace = True)
        # str_col = []
        # for col in df.columns.tolist():
        #     if col.dtype == object:
        #         str_col.append(col)
        # df.fillna(0,inplace = True)
        # for col in str_col:
        #     df[col].replace(0,'',inplace = True)
        self.variables["trans_single_portrait"] = df.to_json( orient = 'records')[1:-1]