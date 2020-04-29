# @Time : 2020/4/24 9:48 AM 
# @Author : lixiaobo
# @File : data_prepared_processor.py 
# @Software: PyCharm

# 数据准备阶段， 避免同一数据多次IO交互
from exceptions import DataPreparedException
from mapping.module_processor import ModuleProcessor
from util.mysql_reader import sql_to_df


class DataPreparedProcessor(ModuleProcessor):

    def process(self):
        print("DataPreparedProcessor process")
        # credit_parse_request表
        report_id = self._credit_parse_request_extract()
        # 表数据转换为DataFrame
        self.table_record_to_df("credit_base_info", report_id)
        self.table_record_to_df("pcredit_loan", report_id)
        self.table_record_to_df("pcredit_repayment", report_id)
        self.table_record_to_df("pcredit_default_info", report_id)
        self.table_record_to_df("pcredit_query_record", report_id)
        self.table_record_to_df("pcredit_person_info", report_id)
        self.table_record_to_df("pcredit_biz_info", report_id)
        self.table_record_to_df("pcredit_default_info", report_id)
        self.table_record_to_df("pcredit_live", report_id)
        self.table_record_to_df("pcredit_phone_his", report_id)
        self.table_record_to_df("pcredit_loan", report_id)
        self.table_record_to_df("pcredit_repayment", report_id)
        self.table_record_to_df("pcredit_special", report_id)
        self.table_record_to_df("pcredit_force_execution_record", report_id)
        self.table_record_to_df("pcredit_info", report_id)
        self.table_record_to_df("pcredit_profession", report_id)

        # 入参base_info的信息
        self._basic_info_extract()

    # 基本入参
    def _basic_info_extract(self):
        extra_param = self.origin_data["extraParam"]
        marry_state = extra_param.get("marryState")
        postal_address = extra_param.get("postalAddress")
        house_address = extra_param.get("houseAddress")
        live_address = extra_param.get("liveAddress")
        spouse_name = extra_param.get("spouseName")
        spouse_id_no = extra_param.get("spouseIdNo")

        self.cached_data["basicMarryState"] = marry_state
        self.cached_data["basicPostalAddress"] = postal_address
        self.cached_data["basicHouseAddress"] = house_address
        self.cached_data["basicLiveAddress"] = live_address

        self.cached_data["spouseName"] = spouse_name
        self.cached_data["spouseIdNo"] = spouse_id_no

        credit_base_df = self.cached_data["credit_base_info"]
        self.cached_data["report_time"] = credit_base_df.iloc[0].report_time
        self.cached_data["id_card_no"] = credit_base_df.iloc[0].certificate_no

    # credit_parse_request表信息提取
    def _credit_parse_request_extract(self):
        pre_report_req_no = self.origin_data["preReportReqNo"]
        if pre_report_req_no is None:
            raise DataPreparedException(description="入参数字段preReportReqNo为空")

        sql = "select * from credit_parse_request where out_req_no = %(pre_report_req_no)s"

        df = sql_to_df(sql=sql, params={"pre_report_req_no": pre_report_req_no})
        if df.empty:
            raise DataPreparedException(description="没有查得解析记录:" + pre_report_req_no)

        record = df.iloc[0]
        if "DONE" != record.process_status:
            raise DataPreparedException(description="报告解析状态不正常，操作失败：" + pre_report_req_no + " Status:" + record.process_status)

        report_id = df.iloc[0]["report_id"]

        self.cached_data["credit_parse_request"] = record
        self.cached_data["report_id"] = df.iloc[0]["report_id"]
        return report_id

    # report_id对应的各表的记录获取
    def table_record_to_df(self, table_name, report_id):
        sql = "select * from " + table_name + " where report_id = %(report_id)s"
        df = sql_to_df(sql, params={"report_id": report_id})
        self.cached_data[table_name] = df
