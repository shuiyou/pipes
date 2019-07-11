import datetime

from mapping.tranformer import Transformer


class V00000(Transformer):
    """
    纯展示信息
    """

    def __init__(self) -> None:
        super().__init__()
        self.variables = {
            'base_date': datetime.datetime.now().strftime('%Y-%m-%d'),
            'base_idno': '',
            'base_gender': 0,
            'base_age': 0,
            'base_black': 0,
            'base_type': 'PERSON'
        }


    def transform(self):
        pass
