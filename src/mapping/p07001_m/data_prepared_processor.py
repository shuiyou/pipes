# @Time : 2020/4/24 9:48 AM 
# @Author : lixiaobo
# @File : data_prepared_processor.py 
# @Software: PyCharm

# 数据准备阶段， 避免同一数据多次IO交互
from mapping.module_processor import ModuleProcessor


class DataPreparedProcessor(ModuleProcessor):

    def process(self):
        print("DataPreparedProcessor process")
