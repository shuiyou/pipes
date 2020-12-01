from mapping.module_processor import ModuleProcessor
import pandas as pd
import datetime

class StrategyInputProcessor(ModuleProcessor):

    def process(self):
        self.general_info()
        self.outline_info()
        self.assets_info()
        self.loan_detail_info()
        self.rr_info()
        self.pub_info()



    def general_info(self):
        df = self.cached_data.get("ecredit_generalize_info")

        industry = df.ix[0,'industry']
        launch_year = df.ix[0,'launch_year']
        life_status = df.ix[0, 'life_status']

        risk_industy_list = [
            "货币金融服务",
            "资本市场服务",
            "保险业",
            "其他金融业",
            "中国共产党机关",
            "国家机构",
            "人民政协、民主党派",
            "社会保障",
            "群众团体、社会团体和其他成员组织",
            "基层群众自治组织及其他组织",
            "国际组织"
        ]

        if industry in risk_industy_list:
            self.variables["care_industry"] = 1
        try:
            self.variables["keep_year"] =  datetime.datetime.now().year - int(launch_year)
        except:
            self.variables['keep_year'] = None
        if life_status == "非正常营业":
            self.variables["abnorm_status"] = 1


    def outline_info(self):
        df = self.cached_data.get("ecredit_info_outline")

        self.variables["on_loan_cnt"] = df.ix[0,'remain_loan_org_num']


    def assets_info(self):

        df = self.cached_data.get("ecredit_assets_outline")
        if not df.empty:
            self.variables["asset_dispose_amt"] = df.ix[0,'dispose_blanace']
            self.variables["advance_amt"] = df.ix[0,'advance_blanace']
            self.variables["overdue_prin"] = df.ix[0,'overdue_principal']
            self.variables["overdue_interest"] = df.ix[0,'overdue_interest']


    def loan_detail_info(self):

        df1 = self.cached_data.get("ecredit_loan")

        df2 = pd.merge(self.cached_data.get("ecredit_credit_biz")[['id']],
                       self.cached_data.get("ecredit_draft_lc"),
                       left_on="id",right_on="biz_id"
                       )
        # 未结清部分
        bal1 = df1[df1.settle_status.str.contains("未结清|被追偿")]['balance'].sum()
        bal2 =df2[(df2.settle_status.str.contains("未结清"))
                &(df2.biz_type.str.contains("贴现"))]['balance'].sum()

        if self.cached_data["mamage_amt"] is not None and self.cached_data["mamage_amt"] > 0:
            self.variables["on_loan_prop"]  = (bal1 + bal2) / self.cached_data["mamage_amt"]


        risk_2 = df1[(df1.settle_status.str.contains("未结清|被追偿"))
                                            & (df1.category.str.contains("关注"))].shape[0] + \
                                        df2[(df2.settle_status.str.contains("未结清"))
                                            & (df2.category.str.contains("关注"))].shape[0]


        risk_3 =  df1[(df1.settle_status.str.contains("未结清|被追偿"))
                                            &(df1.category.str.contains("次级"))].shape[0] + \
                                        df2[(df2.settle_status.str.contains("未结清"))
                                            &(df2.category.str.contains("次级"))].shape[0]

        risk_4 = df1[(df1.settle_status.str.contains("未结清|被追偿"))
                                            &(df1.category.str.contains("可疑"))].shape[0] + \
                                        df2[(df2.settle_status.str.contains("未结清"))
                                            &(df2.category.str.contains("可疑"))].shape[0]

        risk_5 = df1[(df1.settle_status.str.contains("未结清|被追偿"))
                                            &(df1.category.str.contains("损失"))].shape[0] + \
                                        df2[(df2.settle_status.str.contains("未结清"))
                                            &(df2.category.str.contains("损失"))].shape[0]

        self.variables["care_loan_cnt"] = risk_2
        self.variables["bad_loan_cnt"] = risk_3 + risk_4 + risk_5

        if risk_5 > 0 :
            self.variables["loan_detail"]  = "损失"
        elif risk_4 > 0:
            self.variables["loan_detail"] = "可疑"
        elif risk_3 > 0 :
            self.variables["loan_detail"] = "次级"
        elif risk_2 > 0 :
            self.variables["loan_detail"] = "关注"

        self.variables["postpone_cnt"] = df1[(df1.settle_status.str.contains("未结清|被追偿"))
                                              &(df1.special_briefgv.str.contains("展期"))].shape[0]

        self.variables["app_cnt_recent"]  = df1[(~df1.account_type.str.contains("贴现")
                            &( datetime.datetime.now().date() - df1.loan_date  <=  pd.Timedelta(days=365*3)))].shape[0]


        # 已结清部分

        risk_d_2 = df1[(df1.settle_status.str.contains("已结清"))
                                             & (df1.category.str.contains("关注"))].shape[0] + \
                                         df2[(df2.settle_status.str.contains("已结清"))
                                             & (df2.category.str.contains("关注"))].shape[0]
        risk_d_3 = df1[(df1.settle_status.str.contains("已结清"))
                                             & (df1.category.str.contains("次级"))].shape[0] + \
                                         df2[(df2.settle_status.str.contains("已结清"))
                                             & (df2.category.str.contains("次级"))].shape[0]
        risk_d_4 = df1[(df1.settle_status.str.contains("已结清"))
                                             & (df1.category.str.contains("可疑"))].shape[0] + \
                                         df2[(df2.settle_status.str.contains("已结清"))
                                             & (df2.category.str.contains("可疑"))].shape[0]
        risk_d_5 = df1[(df1.settle_status.str.contains("已结清"))
                                             & (df1.category.str.contains("损失"))].shape[0] + \
                                         df2[(df2.settle_status.str.contains("已结清"))
                                             & (df2.category.str.contains("损失"))].shape[0]


        self.variables["care_done_cnt"] = risk_d_2
        self.variables["bad_done_cnt"] =  risk_d_3 + risk_d_4 + risk_d_5

        if risk_d_5 > 0 :
            self.variables["settled_detail"]  = "损失"
        elif risk_d_4 > 0:
            self.variables["settled_detail"] = "可疑"
        elif risk_d_3 > 0 :
            self.variables["settled_detail"] = "次级"
        elif risk_d_2 > 0 :
            self.variables["settled_detail"] = "关注"

        df3 = pd.merge(self.cached_data.get("ecredit_loan")[['id','settle_status']],
                       self.cached_data.get("ecredit_histor_perfo"),
                       left_on="id",right_on="loan_id"
                       )

        self.variables["history_prin_overdue"]  = df3[(df3.settle_status.str.contains("已结清"))].overdue_amt.sum()


    # 对外担保
    def rr_info(self):
        df1 = self.cached_data.get("ecredit_repay_duty_biz")
        df2 = self.cached_data.get("ecredit_repay_duty_discount")

        risk_rr_2 = df1[df1.category.str.contains("关注")].shape[0] + \
                                        df2[df2.category.str.contains("关注")].shape[0]
        risk_rr_3 = df1[df1.category.str.contains("次级")].shape[0] + \
                                        df2[df2.category.str.contains("次级")].shape[0]
        risk_rr_4 = df1[df1.category.str.contains("可疑")].shape[0] + \
                                        df2[df2.category.str.contains("可疑")].shape[0]
        risk_rr_5 = df1[df1.category.str.contains("损失")].shape[0] + \
                                        df2[df2.category.str.contains("损失")].shape[0]

        self.variables["care_rr_cnt"]  = risk_rr_2
        self.variables["bad_rr_cnt"]  = risk_rr_3 + risk_rr_4 + risk_rr_5

        if risk_rr_5 > 0 :
            self.variables["rr_detail"]  = "损失"
        elif risk_rr_4 > 0:
            self.variables["rr_detail"] = "可疑"
        elif risk_rr_3 > 0 :
            self.variables["rr_detail"] = "次级"
        elif risk_rr_2 > 0 :
            self.variables["rr_detail"] = "关注"


    # 公共信息
    def pub_info(self):
        df1 = self.cached_data.get("ecredit_civil_judgments")
        df2 = self.cached_data.get("ecredit_force_execution")
        loan_case_list = [
             '金融借款合同纠纷',
             '民间借贷纠纷',
             '借款合同纠纷',
             '追偿权纠纷',
             '融资租赁合同纠纷',
             '保险人代位求偿权纠纷',
             '保证合同纠纷',
             '分期付款买卖合同纠纷',
             '小额借款合同纠纷',
             '票据付款请求权纠纷',
             '担保追偿权纠纷',
             '债权转让合同纠纷',
             '金融不良债权追偿纠纷',
             '企业借贷纠纷',
             '票据追索权纠纷',
             '信用卡纠纷'
        ]

        case_detail = []

        case_1 = df2[df2.case_status.str.contains("失信")].shape[0]
        case_2 = df2[df2.case_status.str.contains("限制")].shape[0]
        case_3 = df1[df1.case_subject.isin(loan_case_list)].shape[0] + \
                    df2[df2.case_subject.isin(loan_case_list)].shape[0]

        if case_1 > 0 :
            case_detail.append("执行失信")
        if case_2 > 0 :
            case_detail.append("执行限制")
        if case_3 > 0 :
            case_detail.append("借贷纠纷")

        self.variables["case_detail"] = "、".join(case_detail)

        self.variables["risk_case_cnt"]  = df1[df1.case_subject.isin(loan_case_list)].shape[0] + \
                            df2[(df2.case_subject.isin(loan_case_list))
                                &(df2.case_status.str.contains("失信|限制"))].shape[0]