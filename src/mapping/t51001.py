
# @Time : 2020/6/19 1:52 PM
# @Author : lixiaobo
# @File : t51001.py.py 
# @Software: PyCharm
from mapping.tranformer import Transformer


class T51001(Transformer):
    """
    流水报告决策入参及变量清洗调度中心
    """

    def __init__(self) -> None:
        super().__init__()
        self.variables = {

        }

    def transform(self):
        handle_list = [
        ]

        for handler in handle_list:
            handler.init(self.variables, self.user_name, self.id_card_no, self.origin_data, self.cached_data)
            handler.process()
