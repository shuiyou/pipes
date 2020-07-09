
import pandas as pd
import re


class TransBasic:
    """
    流水标题行处理,将标题行按照正则匹配方式分配到各个对应的列中
    author:汪腾飞
    created_time:20200630
    updated_time_v1:
    """

    def __init__(self, trans_data):
        self.df = trans_data
        self.col_mapping = {
            'time_col': [],
            'amt_col': [],
            'bal_col': [],
            'cur_col': [],
            'opname_col': [],
            'opacc_col': [],
            'opbank_col': [],
            'chn_col': [],
            'typ_col': [],
            'use_col': [],
            'mark_col': []
        }

    def match_title(self):
        self._match_title_col()

    def _match_title_col(self):
        """
        将标题行粗略分配到对应的数据库标题列中
        :return:
        """
        time_pat = re.compile(r'(日期|时间|交易日|记账日|账务日|[Tt]ime)')
        amt_pat = re.compile(r"(收支|转入|转出|金额|发生额|支出|存入|收入|Debit(?!A)|Credit|记账方向|支取|出账|进账|取出|借|贷|汇出|汇入|交易类型)")
        bal_pat = re.compile(r'(余额|[Bb]alance)')
        cur_pat = re.compile(r'(币种|货币|[Cc]urrency)')
        opname_pat = re.compile(r"(?<!本.)(户名|方名称|姓名|单位名称|对方单位|公司名|对手名称|人名称|账号名称)")
        opacc_pat = re.compile(r'(?<!本.)(账号(?!名称)|账户(?!名称|明细|省市)|ID|户口号)')
        opbank_pat = re.compile(r'(?<!本.)((?<!交易)行名|开户行|开户机构|开户网点|银行名称|银行)')
        chn_pat = re.compile(r'(渠道|交易行|受理机构|交易网点|交易机构)')
        typ_pat = re.compile(r'(方式|业务类型|[Tt]ype|业务类别|现转)')
        use_pat = re.compile(r'(用途)')
        mark_pat = re.compile(r'(摘要|说明|附言|备注|[Dd]escription|个性化信息|其他|其它|对方信息)')
        for col in self.df.columns:
            col = str(col)
            if re.search(time_pat, col):
                self.col_mapping['time_col'].append(col)
            elif re.search(amt_pat, col):
                self.col_mapping['amt_col'].append(col)
            elif re.search(bal_pat, col):
                self.col_mapping['bal_col'].append(col)
            elif re.search(cur_pat, col):
                self.col_mapping['cur_col'].append(col)
            elif re.search(opname_pat, col):
                self.col_mapping['opname_col'].append(col)
            elif re.search(opacc_pat, col):
                self.col_mapping['opacc_col'].append(col)
            elif re.search(opbank_pat, col):
                self.col_mapping['opbank_col'].append(col)
            elif re.search(chn_pat, col):
                self.col_mapping['chn_col'].append(col)
            elif re.search(typ_pat, col):
                self.col_mapping['typ_col'].append(col)
            elif re.search(use_pat, col):
                self.col_mapping['use_col'].append(col)
            elif re.search(mark_pat, col):
                self.col_mapping['mark_col'].append(col)
        return
