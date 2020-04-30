from mapping.module_processor import ModuleProcessor
from product.date_time_util import before_n_month_date, before_n_year_date


class PcreditQueryRecordView(ModuleProcessor):

    def process(self):
        self._get_query_record_msg()


    def _get_query_record_msg(self):
        df=self.cached_data.get("pcredit_query_record")
        credit_base_info_df = self.cached_data.get("credit_base_info")
        report_time = credit_base_info_df.loc[0, 'report_time']
        report_time_before_3_month=before_n_month_date(report_time,3)
        df_3_month=df[df['jhi_time']>report_time_before_3_month]
        if not df_3_month.empty:
            #查询信息-近三个月查询记录-查询日期
            self.variables["jhi_time_3m"]=df_3_month.loc[:,'jhi_time'].tolist()
            #查询信息-近三个月查询记录-查询机构
            self.variables["operator_3m"]=df_3_month.loc[:,'operator'].tolist()
            #查询信息-近三个月查询记录-查询原因
            self.variables["reason_3m"]=df_3_month.loc[:,'reason'].tolist()
            #查询信息-近三个月查询记录-保前审查记录条数
            self.variables["guar_query_cnt"]=df_3_month[df_3_month['reason']=='08'].shape[0]
            #查询信息-近三个月查询记录-资信审查记录条数
            self.variables["loan_query_cnt"]=df_3_month[df_3_month['reason'].str.contains('资信审查')].shape[0]
            #查询信息-近三个月查询记录-本人查询记录条数
            self.variables["self_query_cnt"] = df_3_month[df_3_month['reason'].str.contains('本人')].shape[0]
        report_time_before_1_year=before_n_year_date(report_time,1)
        df_1_year=df[df['jhi_time']>report_time_before_1_year]
        if not df_1_year.empty:
            df_1_year_reason=df_1_year[df_1_year['reason'].isin(['01','02'])]
            if not df_1_year_reason.empty:
                #查询信息-近一年贷款审批和贷记卡审批的查询记录查询日期
                self.variables["jhi_time_1y"]=df_1_year_reason.loc[:,'jhi_time'].tolist()
                #查询信息-近一年贷款审批和贷记卡审批的查询记录查询机构
                self.variables["jhi_time_1y"]=df_1_year_reason.loc[:,'operator'].tolist()
                #查询信息-近一年贷款审批和贷记卡审批的查询记录查询原因
                self.variables["reason_1y"] = df_1_year_reason.loc[:, 'reason'].tolist()
            #查询信息-近一年贷款审批和贷记卡审批的查询记录银行查询笔数
            self.variables["bank_query_cnt"]=df_1_year[df_1_year['reason']=='01'].shape[0]
            #查询信息-近一年贷款审批和贷记卡审批的查询记录贷记卡查询笔数
            self.variables["credit_query_cnt"] = df_1_year[df_1_year['reason'] == '02'].shape[0]
