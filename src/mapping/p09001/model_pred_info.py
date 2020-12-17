
import math
from logger.logger_util import LoggerUtil
from mapping.tranformer import Transformer

logger = LoggerUtil().logger(__name__)


class ModelPredInfo(Transformer):

    def __init__(self):
        super().__init__()
        self.variables = {
            'main_spouse_gap': 0,  # 主体配偶等级差
            'loan_amt_pred_avg': 0  # 主体配偶预测额度平均值
        }
        self.full_msg = None

    def transform(self):
        main_level = 0
        spouse_level = 0
        main_amt_pred = 0
        spouse_amt_pred = 0
        subject = self.full_msg.get('subject')
        if subject is None:
            return
        subject_cnt = len(subject)
        for i in range(subject_cnt):
            temp = subject[i]
            temp_relation = temp['queryData']['relation']
            if temp_relation == 'MAIN':
                main_level = temp['strategyResult']['default_risk_level']
                main_amt_pred = temp['strategyResult']['loan_amt_pred']
            elif temp_relation == 'SPOUSE':
                spouse_level = temp['strategyResult']['default_risk_level']
                spouse_amt_pred = temp['strategyResult']['loan_amt_pred']
            else:
                continue
        self.variables['main_spouse_gap'] = main_level - spouse_level
        self.variables['loan_amt_pred_avg'] = math.floor((main_amt_pred + spouse_amt_pred) / 2)
