# @Time : 2020/6/19 2:13 PM 
# @Author : lixiaobo
# @File : 51001.py.py 
# @Software: PyCharm
from mapping.tranformer import Transformer


class V51001(Transformer):
    """
    流水报告变量清洗
    """
    def __init__(self) -> None:
        super().__init__()
        self.variables = {
        }

    def transform(self):
        view_handle_list = [
        ]

        for view in view_handle_list:
            view.init(self.variables, self.user_name, self.id_card_no, self.origin_data, self.cached_data)
            view.process()
