# @Time : 2020/6/19 2:13 PM 
# @Author : lixiaobo
# @File : 51001.py.py 
# @Software: PyCharm
from mapping.tranformer import Transformer
from view.p08001_v.json_s_counterparty_portrait import JsonSingleCounterpartyPortrait
from view.p08001_v.json_u_counterparty_portrait import JsonUnionCounterpartyPortrait


class V51001(Transformer):
    """
    流水报告变量清洗
    """
    def __init__(self) -> None:
        super().__init__()

    def transform(self):
        view_handle_list = [
        ]

        if self.cached_data["single"]:
            view_handle_list.append(JsonSingleCounterpartyPortrait())
        else:
            view_handle_list.append(JsonUnionCounterpartyPortrait())

        for view in view_handle_list:
            view.init(self.variables, self.user_name, self.id_card_no, self.origin_data, self.cached_data)
            view.process()
