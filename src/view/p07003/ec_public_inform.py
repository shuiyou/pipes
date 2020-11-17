from mapping.grouped_tranformer import GroupedTransformer, invoke_each


class EcPublicInform(GroupedTransformer):

    def invoke_style(self) -> int:
        return invoke_each

    def group_name(self):
        return "ec_public_inform"

    def __init__(self) -> None:
        super().__init__()
        self.variables = {
            "handle_inst": [],
            "handle_date": [],
            "handle_reason": [],
            "handle_no": [],
            "handle_type": [],
            "handle_amt": [],
            "handle_remark": [],
            "handle_status": [],
            "staff_size": 0,
            "pay_status": "",
            "arrears": 0,
            "recent_pay_date": ""
        }

    def transform(self):
        pass