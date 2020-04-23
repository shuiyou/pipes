from mapping.module_processor import ModuleProcessor


# 和CCS数据比较相关的变量清洗


class CompareDataProcessor(ModuleProcessor):
    def process(self):
        print("CompareDataProcessor process")
