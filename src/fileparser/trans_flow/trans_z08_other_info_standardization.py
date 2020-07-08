

class TransactionOtherInfo:
    """
    将流水文件中所有其他类型信息(包括:交易币种,交易渠道,交易类型,交易用途,交易备注)标准化
    author:汪腾飞
    created_time:20200630
    updated_time_v1:
    """

    def __init__(self, trans_data, col_mapping):
        self.df = trans_data
        self.col_mapping = col_mapping

    def trans_info_match(self):
        self._trans_info_match('cur_col', 'currency')
        self._trans_info_match('chn_col', 'trans_channel')
        self._trans_info_match('typ_col', 'trans_type')
        self._trans_info_match('use_col', 'trans_use')
        self._remark_match()

    def _trans_info_match(self, trans_info, col_name):
        length = len(self.col_mapping[trans_info])
        if length:
            string = ''
            for col in self.col_mapping[trans_info]:
                string += "self.df['" + col + "'].fillna('').astype(str)+"
            string = string[:-1]
            self.df[col_name] = eval(string)
        else:
            self.df[col_name] = ''
        return

    def _remark_match(self):
        length = len(self.col_mapping['mark_col'])
        if length:
            string = ''
            for col in self.col_mapping['mark_col']:
                if '对方信息' not in col:
                    string += "self.df['" + col + "'].fillna('').astype(str)+"
                else:
                    string += "self.df['" + col + "'].fillna('').astype(str).apply(lambda x:x.split(':',1)[-1])+"
                    if len(self.df['opponent_name'].value_counts().index) == 1:
                        self.df['opponent_name'] = self.df[col].fillna('').astype(str).\
                            apply(lambda x: x.split(':', 1)[0])
            string = string[:-1]
            self.df['remark'] = eval(string)
        else:
            self.df['remark'] = ''
        return
