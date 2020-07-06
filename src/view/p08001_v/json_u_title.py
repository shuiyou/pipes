from view.TransFlow import TransFlow

class JsonUnionTitle(TransFlow):

    def process(self):
        self.create_u_title()

    def create_u_title(self):

        cusName = self.origin_data['']
        appAmt = self.origin_data['']
        account_list = self.origin_data['']
        relation_list = self.origin_data['']


        self.variables['表头']  = "{\"cusName\":" + cusName  \
                                + ",\"appAmt\":" + appAmt  \
                                + ",\"流水信息\":" + account_list \
                                + ",\"关联人\":"  + relation_list  + "}"