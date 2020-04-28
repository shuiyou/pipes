# @Time : 2020/4/28 2:52 PM 
# @Author : lixiaobo
# @File : credit_info_processor.py.py 
# @Software: PyCharm
from mapping.module_processor import ModuleProcessor

# credit开头的相关变量


class CreditInfoProcessor(ModuleProcessor):
    def process(self):
        self._credit_fiveLevel_a_level_cnt()
        self._credit_now_overdue_money()
        self._credit_overdue_max_month()
        self._credit_overdrawn_2card()
        self._credit_overdue_5year()

    # 贷记卡五级分类存在“可疑、损失”
    def _credit_fiveLevel_a_level_cnt(self):
        df = self.cached_data.get("pcredit_loan")
        if df is None or df.empty:
            return

        df = df.query('account_type in ["04", "05"] and latest_category in ["4", "5"]')
        if df is not None:
            self.variables["credit_fiveLevel_a_level_cnt"] = df.shape[0]

    # 贷记卡当前逾期金额
    def _credit_now_overdue_money(self):
        # TODO
        pass

    # 贷记卡最大连续逾期月份数
    def _credit_overdue_max_month(self):
        # TODO
        pass

    # 贷记卡总透支率达80%且存在2张贷记卡最低额还款
    def _credit_overdrawn_2card(self):
        # TODO
        pass

    # 总计贷记卡5年内逾期次数
    def _credit_overdue_5year(self):
        # TODO
        pass
