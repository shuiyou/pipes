
from mapping.module_processor import ModuleProcessor
import datetime
import pandas as pd
from util.mysql_reader import sql_to_df

class TransModuleProcessor(ModuleProcessor):

    def __init__(self):
        super().__init__()

        self.reqno = self.cached_data.get("input_param")[0]["preReportReqNo"]

        sql = """
            select *
            from trans_u_flow_portrait
            where report_req_no = %(report_req_no)s
        """
        self.trans_u_flow_portrait = sql_to_df( sql = sql,
                                                params= {"report_req_no": self.reqno})

