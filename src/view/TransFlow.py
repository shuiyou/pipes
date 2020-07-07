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
        self.variables = {}