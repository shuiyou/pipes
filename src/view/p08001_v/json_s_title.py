from view.TransFlow import TransFlow

class JsonSingleTitle(TransFlow):

    def process(self):
        self.create_s_title()

    def create_s_title(self):
        cusName = self.origin_data['']
        bankName = self.origin_data['']
        bankAccount = self.origin_data['']
        startEndDate = self.origin_data['']
        relation_list = self.origin_data[''].to_json(orient = 'records')


        self.variables["表头"] = "{\"cusName\":" + cusName  \
                               +  ",\"流水信息\":{\"bankName\":" + bankName \
                               + ",\"bankAccount\":" + bankAccount \
                               + ",\"startEndDate\":" + startEndDate + "},"  \
                               + "\"关联人\":" + relation_list + "}"