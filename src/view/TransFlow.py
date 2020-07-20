from mapping.module_processor import ModuleProcessor
from util.mysql_reader import sql_to_df


class TransFlow(ModuleProcessor):

    def __init__(self):
        super().__init__()
        # self.db = self._db()
        self.account_id = None
        self.cusName = None
        self.bankName = None
        self.bankAccount = None
        self.idno = None
        self.reqno = None
        self.appAmt = None
        self.guarantor_list = []
        self.variables = {}

    def init(self, variables, user_name, id_card_no, origin_data, cached_data):
        super().init(variables, user_name, id_card_no, origin_data, cached_data)
        for i in self.cached_data.get("input_param"):
            if i["relation"] == "MAIN":
                self.cusName = i["name"]
                self.bankName = i["extraParam"]["accounts"][0]["bankName"]
                self.bankAccount = i["extraParam"]["accounts"][0]["bankAccount"]
                self.idno = i["idno"]
                self.reqno = i["preReportReqNo"]
                self.appAmt = i["applyAmo"]

                sql = """
                    select account_id
                    from trans_apply ap
                    left join 
                    trans_account ac
                    on ap.account_id = ac.id
                    where ap.report_req_no = %(report_req_no)s 
                    and ap.id_card_no = %(id_card_no)s 
                    and ac.account_no = %(account_no)s
                """
                account_df = sql_to_df(sql=sql,
                               params={"report_req_no":self.reqno,
                                       "id_card_no":self.idno,
                                       "account_no":self.bankAccount})
                if not account_df.empty:
                    self.account_id =  int(account_df.values[0][0])

            if i["relation"] == "GUARANTOR":
                self.guarantor_list.append(i["name"])