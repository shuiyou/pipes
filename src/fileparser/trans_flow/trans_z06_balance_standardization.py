
import decimal
import re


class TransactionBalance:
    """
    将流水文件中交易余额标准化
    author:汪腾飞
    created_time:20200630
    updated_time_v1:
    """

    def __init__(self, trans_data, col_mapping):
        self.df = trans_data
        self.bal_col = col_mapping['bal_col']
        self.basic_status = True
        self.resp = {
            "resCode": "0",
            "resMsg": "成功",
            "data": {
                "warningMsg": []
            }
        }

    def _notnull_max(self):
        length = len(self.df)
        cnt = self.df[self.bal_col].count()
        max_v = -1
        max_col = ''
        for k, v in cnt.items():
            if v == length:
                return k
            elif v > max_v:
                max_v = v
                max_col = k
        return max_col

    def _remove_bal_col(self):
        """
        去除余额列中不符合规范的列
        :return:
        """
        length = len(self.bal_col)
        for index in range(-length, 0):
            col = self.bal_col[index]
            if re.search(r"(上|Last|last|前)", col):
                self.bal_col.remove(col)
        return

    def _one_col_match(self, col):
        self.df['account_balance'] = self.df[col].fillna('').astype(str). \
                apply(lambda x: re.sub(r'[^\d.]', '', x)).replace('', '0').astype(float)
        return

    def balance_match(self):
        self._remove_bal_col()
        col = self._notnull_max()
        if col != '':
            self._one_col_match(col)
        else:
            self.basic_status = False
            self.resp['resCode'] = '20'
            self.resp['resMsg'] = '解析失败'
            self.resp['data']['warningMsg'] = ['该流水中未找到交易余额列,请检查后再上传']
        return

    # 流水验真
    def balance_sequence_check(self):
        last = -1
        for row in self.df.itertuples():
            if last == -1:
                last = getattr(row, 'account_balance')
            else:
                trans_amt = getattr(row, 'trans_amt')
                trans_bal = getattr(row, 'account_balance')
                if float(decimal.Decimal(str(trans_amt)) + decimal.Decimal(str(last))) != trans_bal:
                    self.basic_status = False
                    self.resp['resCode'] = '22'
                    self.resp['resMsg'] = '验真失败'
                    self.resp['data']['warningMsg'] = ['该流水存在余额与交易金额不匹配的行,该流水为假流水']
                    return
