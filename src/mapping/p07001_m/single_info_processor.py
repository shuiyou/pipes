# @Time : 2020/4/28 3:08 PM 
# @Author : lixiaobo
# @File : single_info_processor.py.py 
# @Software: PyCharm
from mapping.module_processor import ModuleProcessor

# single开头的相关的变量


class SingleInfoProcessor(ModuleProcessor):
    def process(self):
        self._single_house_overdue_2year_cnt()
        self._single_car_overdue_2year_cnt()
        self._single_consume_overdue_2year_cnt()

    # 单笔房贷近2年内最大逾期次数
    def _single_house_overdue_2year_cnt(self):
        pass

    # 单笔车贷近2年内最大逾期次数
    def _single_car_overdue_2year_cnt(self):
        pass

    # 单笔消费性贷款近2年内最大逾期次数
    def _single_consume_overdue_2year_cnt(self):
        pass
