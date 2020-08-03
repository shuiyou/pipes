
import pandas as pd
import re


class TransactionAmt:
    """
    将流水文件中交易金额标准化
    author:汪腾飞
    created_time:20200630
    updated_time_v1:
    """

    def __init__(self, trans_data, col_mapping):
        self.df = trans_data
        self.amt_col = col_mapping['amt_col']
        self.basic_status = True
        self.resp = {
            "resCode": "0",
            "resMsg": "成功",
            "data": {
                "warningMsg": []
            }
        }

    def _find_tag_col(self):
        """
        在金额列中寻找是否有标签列,如果存在标签列则新增一列'tag'将标签列对应的出账表示为-1,进账表示为1
        :return:
        """
        length = len(self.amt_col)
        for index in range(-length, 0):
            col = self.amt_col[index]
            temp = self.df[col].value_counts()
            index = ''.join([str(_) for _ in temp.index])
            if len(temp) == 2 and re.search(r'[借贷往来出入取存收付]', index):
                tag = col
                self.df['tag'] = self.df[tag].astype(str).apply(lambda x: re.sub(r'.*[借出支往取付].*', '1', x)).\
                    apply(lambda x: 1 if x != '1' else -1)
                self.amt_col.remove(col)
                return 1
            if col == '交易类型':
                self.amt_col.remove(col)
                self.amt_col.append(col)
        return 0

    def _remove_amt_col(self):
        """
        去除金额列中不符合规范的列
        :return:
        """
        length = len(self.amt_col)
        for index in range(-length, 0):
            col = self.amt_col[index]
            # 将每个金额列数据类型都替换为字符串,且将字符串中的非数字小数点负号替换为空,或者以负号结尾的数据替换为空
            self.df[col] = self.df[col].fillna('').astype(str).apply(lambda x: re.sub(r'[^\d.-]|.*-$', '', x))
            # 若转化过后整列全都是空字符串则删除该列
            temp = self.df[col].value_counts()
            if len(temp) == 1 and temp.index[0] == '':
                self.amt_col.remove(col)
        # 若金额列依然包含两列以上,则筛选其中的对立列
        if len(self.amt_col) > 2:
            string = ''.join([str(x) for x in self.amt_col])
            if re.search(r'[收入存贷进]|Credit', string) and re.search(r'[支出取借付]|Debit', string):
                for index in range(-len(self.amt_col), 0):
                    col = self.amt_col[index]
                    if not (re.search(r'(收|入|存|贷|进|Credit)', col) or re.search(r'(支|出|取|借|付|Debit)', col)):
                        self.amt_col.remove(col)
                    elif len(self.df[col].value_counts().index) == 1:
                        self.amt_col.remove(col)
        return

    def _one_col_match(self, col: str, col_name='trans_amt'):
        """
        将对应的金额列转化为标准浮点型数据
        :param col: 需要转化的列名
        :param col_name: 转化后的列名
        :return:
        """
        self.df[col_name] = self.df[col].replace('', '0').astype(float)
        return

    def _multi_col_match(self):
        """
        交易金额列存在多列时的处理
        :return:
        """
        tag = self._find_tag_col()
        self._remove_amt_col()
        self.df[self.amt_col] = self.df[self.amt_col].replace('', '0').astype(float)
        length = len(self.amt_col)
        if length == 1:
            if tag:
                if self.df.loc[self.df['tag'] == -1][self.amt_col[0]].sum() < 0:
                    self.df['trans_amt'] = self.df[self.amt_col[0]]
                else:
                    self.df['trans_amt'] = self.df['tag']*self.df[self.amt_col[0]]
            else:
                if len(self.df.loc[self.df[self.amt_col[0]] < 0]) > 0:
                    self.df['trans_amt'] = self.df[self.amt_col[0]]
                else:
                    raise ValueError("未找到交易金额列")
        elif length == 2:
            if tag:
                self.df['trans_amt'] = self.df[self.amt_col[0]] + self.df[self.amt_col[1]]
                if self.df.loc[self.df['tag'] == -1]['trans_amt'].sum() >= 0:
                    self.df['trans_amt'] = self.df['tag'] * self.df['trans_amt']
            else:
                if re.search(r'(借|出|支|往|Debit)', self.amt_col[0]):
                    # neg = len(self.df.loc[self.df[self.amt_col[0]] < 0])
                    neg = self.df[self.amt_col[0]].sum()
                    multi = 1 if neg < 0 else -1
                    self.df['trans_amt'] = multi * self.df[self.amt_col[0]] + self.df[self.amt_col[1]]
                elif re.search(r'(借|出|支|往|Debit)', self.amt_col[1]):
                    # neg = len(self.df.loc[self.df[self.amt_col[1]] < 0])
                    neg = self.df[self.amt_col[1]].sum()
                    multi = 1 if neg < 0 else -1
                    self.df['trans_amt'] = self.df[self.amt_col[0]] + multi * self.df[self.amt_col[1]]
                else:
                    raise ValueError("存在多列无法区分的交易金额列")
        else:
            raise ValueError("未找到交易金额列")
        return

    def amt_match(self):
        length = len(self.amt_col)
        try:
            if length == 1:
                self._one_col_match(self.amt_col[0])
            else:
                self._multi_col_match()
        except ValueError as e:
            self.basic_status = False
            self.resp['resCode'] = '20'
            self.resp['resMsg'] = '解析失败'
            self.resp['data']['warningMsg'].append(e)
