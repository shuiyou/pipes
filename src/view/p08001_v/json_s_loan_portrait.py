from view.TransFlow import TransFlow

class JsonSingleLoanPortrait(TransFlow):

    def process(self):
        self.read_single_loan_pt()

    def connect_json(self,json):
        string = ''
        for text in json:
            string += text
        return string[:-1]

    def read_single_loan_pt(self):

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
        # string = self.connect_json(json)

        self.variables["trans_single_loan_portrait"] = "{" + self.connect_json(json) + "}"