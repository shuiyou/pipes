import decimal
import re


class TransactionBalance:
    """
    将流水文件中交易余额标准化
    author:汪腾飞
    created_time:20200630
    updated_time_v1:20200709,流水验真顺序为时间顺序
    updated_time_v2:20200818,进行流水验真时先按照原流水顺序验真,
        若不通过再根据时间顺序(排序方式分别按照日期顺序+时间顺序,日期顺序+时间逆序)
    updated_time_v3:20200918,允许上传余额为负值的流水,依然需要识别形如“-      ”的数据
    """

    def __init__(self, trans_data, col_mapping, sort_list):
        self.df = trans_data
        self.bal_col = col_mapping['bal_col']
        self.basic_status = True
        self.sort_list = sort_list
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
                apply(lambda x: re.sub(r'[^\d.-]|.*-$|.+-.+', '', re.sub(r'\s', '', x))).replace('', '0').astype(float)
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

    def balance_check(self):
        self.df = self.df[self.df['trans_amt'] != 0]
        self.df['last_trans_amt'] = self.df.apply(
            lambda x: float(decimal.Decimal(str(x['account_balance'])) - decimal.Decimal(str(x['trans_amt']))), axis=1)
        sort_df = self.df.sort_values(by=self.sort_list, ascending=True)
        sort_df['trans_date'] = sort_df['trans_time'].apply(lambda x: x.date())
        date_list = sort_df['trans_date'].unique()
        for d in date_list:
            temp_date_df = sort_df[sort_df['trans_date'] == d]
            pass

    # 流水验真逻辑
    def _balance_check(self, df):
        last = -1
        for row in df.itertuples():
            if last == -1:
                last = getattr(row, 'account_balance')
            else:
                index = getattr(row, 'Index')
                trans_amt = getattr(row, 'trans_amt')
                trans_bal = getattr(row, 'account_balance')
                trans_channel = getattr(row, 'trans_channel')
                trans_type = getattr(row, 'trans_type')
                trans_use = getattr(row, 'trans_use')
                remark = getattr(row, 'remark')
                concat_str = trans_channel + trans_type + trans_use + remark
                # 若某行备注是利息,且其值为负值则直接返回
                # if re.search('(?<!还)利息|结息|入息|增值息|存息', concat_str) is not None and trans_amt < 0:
                #     self.basic_status = False
                #     self.resp['resCode'] = '22'
                #     self.resp['resMsg'] = '验真失败'
                #     self.resp['data']['warningMsg'] = ['该流水存在利息为负的行,该流水为假流水']
                #     return
                if float(decimal.Decimal(str(trans_amt)) + decimal.Decimal(str(last))) != trans_bal:
                    # 若某行不满足余额校验,但是该行属于冲正,抹账,退账,则将交易金额乘以-1重新验证一次,若通过则继续往下校验
                    if re.search('冲正|抹账|退账|抹帐|退帐|冲帐|冲账', concat_str) is not None:
                        if float(decimal.Decimal(str(-trans_amt)) + decimal.Decimal(str(last))) == trans_bal:
                            df.loc[index, 'trans_amt'] = -trans_amt
                            last = trans_bal
                            continue
                    return
                last = trans_bal
        return df

    # 进行数次不同顺序的验真
    def balance_sequence_check(self):
        self.df = self.df[self.df['trans_amt'] != 0]
        sort_df1 = self.df.copy()
        sort_df1['index'] = list(range(len(self.df)))
        self.sort_list.append('index')
        # 1.流水文件中原有顺序顺序验真
        result1 = self._balance_check(sort_df1)
        if result1 is not None:
            self.df = result1
            return
        elif not self.basic_status:
            return
        # 2.流水文件中原有顺序逆序验真
        sort_df2 = sort_df1.sort_values(by='index', ascending=False)
        result2 = self._balance_check(sort_df2)
        if result2 is not None:
            self.df = result2
            self.df.reset_index(drop=True, inplace=True)
            return
        elif not self.basic_status:
            return
        # 3.将流水文件按照交易日期,交易时间,index,顺序,顺序,顺序排序后进行验真(可能存在日期,时间均相同的两条流水)
        ascending_list3 = [True] * len(self.sort_list)
        sort_df3 = sort_df1.sort_values(by=self.sort_list, ascending=ascending_list3)
        result3 = self._balance_check(sort_df3)
        if result3 is not None:
            self.df = result3
            self.df.reset_index(drop=True, inplace=True)
            return
        elif not self.basic_status:
            return
        # 4.将流水文件按照交易日期,交易时间,index,顺序,顺序,逆序排序后进行验真(可能存在日期,时间均相同的两条流水)
        ascending_list3[-1] = False
        sort_df4 = sort_df1.sort_values(by=self.sort_list, ascending=ascending_list3)
        result4 = self._balance_check(sort_df4)
        if result4 is not None:
            self.df = result4
            self.df.reset_index(drop=True, inplace=True)
            return
        elif not self.basic_status:
            return
        # 5.上述逻辑都没有通过,则验真失败
        self.basic_status = False
        self.resp['resCode'] = '22'
        self.resp['resMsg'] = '验真失败'
        self.resp['data']['warningMsg'] = ['该流水存在余额与交易金额不匹配的行,该流水为假流水']
        return
