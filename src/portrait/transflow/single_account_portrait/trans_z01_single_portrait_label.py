
from portrait.transflow.single_account_portrait.trans_flow import transform_class_str
from portrait.transflow.single_account_portrait.trans_mapping import base_type_mapping
from fileparser.trans_flow.trans_config import UNSTABLE_DENSITY, UNUSUAL_TRANS_AMT, MIN_PRIVATE_LENDING, \
    MIN_CONTI_MONTHS, MAX_INTERVAL_DAYS
import pandas as pd
import datetime
import re


class TransSingleLabel:
    """
    单账户标签画像表清洗并落库
    author:汪腾飞
    created_time:20200706
    updated_time_v1:20201125,夜间交易风险和家庭不稳定风险以及民间借贷风险逻辑调整
    """

    def __init__(self, trans_flow):
        self.db = trans_flow.db
        self.query_data_array = trans_flow.query_data_array
        self.report_req_no = trans_flow.report_req_no
        self.account_id = trans_flow.account_id
        self.df = trans_flow.trans_flow_df
        self.user_name = trans_flow.user_name
        self.create_time = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')
        self.label_list = []

    def process(self):
        if self.df is None:
            return
        self._relationship_dict()
        self._loan_type_label()
        self._unusual_type_label()
        self._in_out_order()

        self.save_raw_data()
        self.db.session.add_all(self.label_list)
        self.db.session.commit()

    def _relationship_dict(self):
        """
        生成姓名和关联关系对应的字典,需要将编码形式的关联关系转化为中文关联关系
        :return:
        """
        length = len(self.query_data_array)
        self.relation_dict = dict()
        self.spouse_name = 'None'
        for i in range(length):
            temp = self.query_data_array[i]
            base_type_detail = base_type_mapping.get(temp['baseTypeDetail'])
            self.relation_dict[temp['name']] = base_type_detail
            if base_type_detail in ['借款人配偶', '借款企业实际控制人配偶']:
                self.spouse_name = temp['name']

    def _loan_type_label(self):
        """
        包括交易对手类型标签opponent_type,贷款类型标签loan_type,是否还款标签is_repay,是否结息标签is_interest
        是否结息前一周标签is_before_interest_repay
        :return:
        """
        concat_list = ['opponent_name', 'trans_channel', 'trans_type', 'trans_use', 'remark']
        self.df[concat_list] = self.df[concat_list].fillna('').astype(str)
        # 交易对手标签赋值,1个人,2企业,其他为空
        self.df['opponent_type'] = self.df['opponent_name'].apply(self._opponent_type)
        self.df['year_month'] = self.df['trans_time'].apply(lambda x: format(x, '%Y-%m'))
        self.df['year'] = self.df['trans_time'].apply(lambda x: x.year)
        self.df['month'] = self.df['trans_time'].apply(lambda x: x.month)
        self.df['day'] = self.df['trans_time'].apply(lambda x: x.day)
        # 将字符串列合并到一起
        self.df['concat_str'] = self.df.apply(lambda x: ';'.join(x[concat_list]), axis=1)
        # 贷款类型赋值,优先级从上至下
        self.df.loc[self.df['concat_str'].str.contains('消费金融|消费银企|汽车金融|陆金所|微粒贷|花呗|借呗|360还款|消费贷款'),
                    'loan_type'] = '消金'
        self.df.loc[(self.df['concat_str'].str.contains('银行贷款|银行.*放款|放款.*银行')) &
                    (pd.isnull(self.df.loan_type)), 'loan_type'] = '银行'
        self.df.loc[(self.df['concat_str'].str.contains('融资租赁|国际租赁|金融租赁')) &
                    (pd.isnull(self.df.loan_type)), 'loan_type'] = '融资租赁'
        self.df.loc[(self.df['concat_str'].str.contains('担保')) &
                    (pd.isnull(self.df.loan_type)), 'loan_type'] = '担保'
        self.df.loc[(self.df['concat_str'].str.contains('保理')) &
                    (pd.isnull(self.df.loan_type)), 'loan_type'] = '保理'
        self.df.loc[~(self.df['concat_str'].str.contains('小额贷记来账')) &
                    (self.df['concat_str'].str.contains('小额贷|小贷|企业贷|典当|互联网信息咨询')) &
                    (pd.isnull(self.df.loan_type)), 'loan_type'] = '小贷'
        self.df.loc[
            (self.df['concat_str'].str.contains('信托|小微|信贷.*过渡户|过渡户.*信贷|财务.*公司|公司.*财务|资金互助社|金融.*公司|公司.*金融|经济合作社')) &
            (pd.isnull(self.df.loan_type)), 'loan_type'] = '其他金融'
        self.df.loc[(self.df['trans_amt'].apply(lambda x: abs(x)) > MIN_PRIVATE_LENDING) &
                    (((self.df['concat_str'].str.contains('信贷|融资|垫款|放款|个人.*贷|抵押|现金分期|借|还|本金')) &
                      (~self.df['concat_str'].str.contains('借支'))) |
                     ((self.df['trans_amt'] < 0) & (self.df['concat_str'].str.contains('利息|结息')))) &
                    (pd.isnull(self.df.loan_type)), 'loan_type'] = '民间借贷'
        amt_group = self.df[
            self.df['trans_amt'].apply(lambda x: abs(x)) > MIN_PRIVATE_LENDING
        ].groupby(['opponent_name', 'trans_amt'], as_index=False).agg({'month': len})
        amt_group = amt_group[amt_group['month'] >= MIN_CONTI_MONTHS]
        if amt_group.shape[0] > 0:
            for row in amt_group.itertuples():
                temp_name = getattr(row, 'opponent_name')
                temp_amt = getattr(row, 'trans_amt')
                temp_df = self.df[(self.df['opponent_name'] == temp_name) &
                                  (self.df['trans_amt'] == temp_amt)]
                temp_df.reset_index(drop=False, inplace=True)
                last_month = temp_df['trans_time'].tolist()[0]
                temp_df.loc[0, 'conti'] = 1
                conti_list = set()
                temp_cnt = 1
                now_index = 1
                for index in temp_df.index.tolist()[1:]:
                    this_month = temp_df.loc[index, 'trans_time']
                    temp_interval = (this_month.year - last_month.year) * 12 + this_month.month - last_month.month
                    if temp_interval <= 1:
                        temp_df.loc[index, 'conti'] = now_index
                        if temp_interval == 1:
                            temp_cnt += 1
                            if temp_cnt >= MIN_CONTI_MONTHS:
                                conti_list.add(now_index)
                    else:
                        now_index += 1
                        temp_df.loc[index, 'conti'] = now_index
                        temp_cnt = 1
                    last_month = this_month
                conti_list = list(conti_list)
                if len(conti_list) == 0:
                    continue
                for i in conti_list:
                    conti_df = temp_df[temp_df['conti'] == i]
                    max_interval = conti_df['day'].max() - conti_df['day'].min()
                    if max_interval <= MAX_INTERVAL_DAYS:
                        if 'loan_type' not in conti_df.columns:
                            loan_type = '民间借贷'
                        else:
                            conti_type_df = conti_df[pd.notna(conti_df['loan_type'])]
                            if conti_type_df.shape[0] > 0:
                                loan_type = conti_type_df['loan_type'].tolist()[0]
                            else:
                                loan_type = '民间借贷'
                        self.df.loc[conti_df['index'].tolist(), 'loan_type'] = loan_type

        # 是否还款标签
        self.df.loc[(pd.notnull(self.df['loan_type'])) & (self.df['trans_amt'] < 0), 'is_repay'] = 1

        # 是否结息标签
        interest_df = self.df.loc[(self.df.month % 3 == 0) &
                                  (self.df.day.isin([20, 21])) &
                                  (self.df.trans_amt > 0) &
                                  ((self.df.opponent_name == '') |
                                   (self.df.opponent_name == self.user_name) |
                                   (self.df.opponent_name.str.contains(
                                       '银行|利息|结息|个人活期结息|批量结息|存息|付息|存款利息'))) &
                                  (self.df.concat_str.str.contains('利息|结息|个人活期结息|批量结息|存息|付息|存款利息')) &
                                  (~self.df.concat_str.str.contains('理财|钱生钱|余额宝|零钱通|招财盈|宜人财富'))]
        interest_df.reset_index(drop=False, inplace=True)
        group_df = interest_df.groupby(by=['year', 'month'], as_index=False).agg({'trans_amt': min})
        index_list = interest_df.loc[group_df.index.to_list(), 'index'].to_list()
        self.df.loc[index_list, 'is_interest'] = 1

        # 是否结息前一周标签
        repay_date_list = self.df[(self.df['is_repay'] == 1) |
                                  (self.df['is_interest'] == 1)]['trans_time'].to_list()
        for repay_date in repay_date_list:
            seven_days_ago = pd.to_datetime((repay_date - datetime.timedelta(days=7)).date())
            self.df.loc[(self.df.trans_time < repay_date) &
                        (self.df.trans_time >= seven_days_ago), 'is_before_interest_repay'] = 1
        self.df.drop(['year', 'month', 'day'], axis=1, inplace=True)

    def _unusual_type_label(self):
        self.df['date'] = self.df['trans_time'].apply(lambda x: datetime.datetime.strftime(x, '%Y-%m-%d'))

        # 异常交易类型之是否整进整出标签
        big_in_out_df = self.df[(self.df.trans_amt.apply(lambda x: abs(x)) >= 200000) &
                                (self.df.trans_amt.apply(lambda x: x % 10000) == 0)]
        big_in_date = big_in_out_df[big_in_out_df.trans_amt > 0]['date'].tolist()
        big_out_date = big_in_out_df[big_in_out_df.trans_amt < 0]['date'].tolist()
        big_in_out_date = list(set(big_in_date).intersection(set(big_out_date)))
        big_in_out_list = big_in_out_df[big_in_out_df.date.isin(big_in_out_date)].index.to_list()
        self.df.loc[big_in_out_list, 'big_in_out'] = 1

        # 异常交易类型之快进快出标签
        self.df['date'] = self.df[['date', 'opponent_name']]. \
            apply(lambda x: x['date'] + x['opponent_name'], axis=1)
        fast_in_out_df = self.df[(self.df.trans_amt.apply(lambda x: abs(x)) >= 500000) &
                                 (self.df.opponent_name != '') &
                                 (~self.df.opponent_name.str.contains('转账|转存|转支'))]
        fast_in_date = fast_in_out_df[fast_in_out_df.trans_amt > 0]['date'].tolist()
        fast_out_date = fast_in_out_df[fast_in_out_df.trans_amt < 0]['date'].tolist()
        fast_in_out_date = list(set(fast_in_date).intersection(set(fast_out_date)))
        fast_in_out_list = fast_in_out_df[fast_in_out_df.date.isin(fast_in_out_date)].index.to_list()
        self.df.loc[fast_in_out_list, 'fast_in_out'] = 1

        # 异常交易类型之是否偶发大额标签
        trans_amt_mean = self.df[(self.df['trans_amt'] > 0) &
                                 (pd.isnull(self.df['loan_type']))]['trans_amt'].mean()
        trans_amt_std = self.df[(self.df['trans_amt'] > 0) &
                                (pd.isnull(self.df['loan_type']))]['trans_amt'].std()
        self.df.loc[self.df.trans_amt > trans_amt_mean + 2 * trans_amt_std, 'accidental_big'] = 1

        # 异常交易之家庭不稳定标签
        unstable_df = self.df[(~self.df['opponent_name'].str.contains(self.spouse_name)) &
                              (self.df['trans_amt'].apply(lambda x: abs(x)).isin(UNUSUAL_TRANS_AMT))]
        unstable_count = unstable_df.groupby('opponent_name', as_index=False).agg({'trans_amt': len})
        unstable_name_list = list(set(unstable_df['opponent_name']))
        unstable_total = self.df[
            self.df['opponent_name'].isin(unstable_name_list)
        ].groupby('opponent_name', as_index=False).agg({'trans_time': len})
        unstable_count = pd.merge(unstable_count, unstable_total, how='left', on='opponent_name')
        if unstable_count.shape[0] > 0:
            unstable_count['density'] = unstable_count['trans_amt'] / unstable_count['trans_time']
            unstable_name_list = unstable_count[unstable_count['density'] >= UNSTABLE_DENSITY]['opponent_name'].tolist()
            unstable_index_list = unstable_df[unstable_df['opponent_name'].isin(unstable_name_list)].index.tolist()
            self.df.loc[unstable_index_list, 'unstable'] = 1

    @staticmethod
    def _opponent_type(op_name):
        if len(op_name) > 6 and re.search('(厂|店|公司|经营部)$', op_name) is not None:
            return 2
        else:
            if len(op_name) <= 15:
                cleaned_name = re.sub(r'[^\u4e00-\u9fa5 *]|支付宝转账|支付宝代发', '', op_name)
                if re.match(r'(支付宝|消费支付宝|淘宝)', cleaned_name):
                    cleaned_name = re.sub(r'(支付宝外部商户|支付宝划账|支付宝| |消费支付宝|淘宝)', '', cleaned_name)
                elif re.match(r'(转账|跨行转出|对私提)', cleaned_name):
                    cleaned_name = cleaned_name.split()[-1]
                else:
                    cleaned_name = re.sub(r' ', '', cleaned_name)
                if 2 <= len(cleaned_name) <= 3:
                    if re.search(r'(转|贷|消费|自取|资金|自定义|友宝|分期|肯德基|代付|麦当劳|携程|红包|活期|房租'
                                 r'|过渡|必胜客|理财|缴费|工资|特约|还款|充值|京东|星巴克|银联|拼多多|爱奇艺|采购'
                                 r'|天猫|租金|提现|淘宝|\*\*|备用|撤销|花呗|借呗)|[费款税账]$', cleaned_name) is None and \
                            re.match(r'[财存天停大柜订百本宝网保北电放还好汇结借跨理利内其上深浙税现中微短发卡随有月油退收快取]',
                                     cleaned_name) is None:
                        return 1

    def _in_out_order(self):
        income_per_df = self.df[(pd.notnull(self.df.opponent_name)) & (self.df.trans_amt > 0) &
                                (self.df.opponent_type == 1)]
        expense_per_df = self.df[(pd.notnull(self.df.opponent_name)) & (self.df.trans_amt < 0) &
                                 (self.df.opponent_type == 1)]
        income_com_df = self.df[(pd.notnull(self.df.opponent_name)) & (self.df.trans_amt > 0) &
                                (self.df.opponent_type == 2)]
        income_com_df = income_com_df[~income_com_df.opponent_name.str.contains(
            '支付宝|财付通|中国移动|中国联通|中国电信')]
        expense_com_df = self.df[(pd.notnull(self.df.opponent_name)) & (self.df.trans_amt < 0) &
                                 (self.df.opponent_type == 2)]
        expense_com_df = expense_com_df[~expense_com_df.opponent_name.str.contains(
            '支付宝|财付通|中国移动|中国联通|中国电信')]
        income_per_cnt_list = income_per_df.groupby(by='opponent_name').agg({'trans_amt': len}). \
            sort_values(by='trans_amt', ascending=False).index.to_list()[:10]
        income_per_amt_list = income_per_df.groupby(by='opponent_name').agg({'trans_amt': sum}). \
            sort_values(by='trans_amt', ascending=False).index.to_list()[:10]
        expense_per_cnt_list = expense_per_df.groupby(by='opponent_name').agg({'trans_amt': len}). \
            sort_values(by='trans_amt', ascending=False).index.to_list()[:10]
        expense_per_amt_list = expense_per_df.groupby(by='opponent_name').agg({'trans_amt': sum}). \
            sort_values(by='trans_amt', ascending=True).index.to_list()[:10]
        income_com_cnt_list = income_com_df.groupby(by='opponent_name').agg({'trans_amt': len}). \
            sort_values(by='trans_amt', ascending=False).index.to_list()[:10]
        income_com_amt_list = income_com_df.groupby(by='opponent_name').agg({'trans_amt': sum}). \
            sort_values(by='trans_amt', ascending=False).index.to_list()[:10]
        expense_com_cnt_list = expense_com_df.groupby(by='opponent_name').agg({'trans_amt': len}). \
            sort_values(by='trans_amt', ascending=False).index.to_list()[:10]
        expense_com_amt_list = expense_com_df.groupby(by='opponent_name').agg({'trans_amt': sum}). \
            sort_values(by='trans_amt', ascending=True).index.to_list()[:10]
        for i in range(len(income_per_cnt_list)):
            self.df.loc[self.df['opponent_name'] == income_per_cnt_list[i], 'income_cnt_order'] = i + 1
        for i in range(len(income_com_cnt_list)):
            self.df.loc[self.df['opponent_name'] == income_com_cnt_list[i], 'income_cnt_order'] = i + 1
        for i in range(len(expense_per_cnt_list)):
            self.df.loc[self.df['opponent_name'] == expense_per_cnt_list[i], 'expense_cnt_order'] = i + 1
        for i in range(len(expense_com_cnt_list)):
            self.df.loc[self.df['opponent_name'] == expense_com_cnt_list[i], 'expense_cnt_order'] = i + 1
        for i in range(len(income_per_amt_list)):
            self.df.loc[self.df['opponent_name'] == income_per_amt_list[i], 'income_amt_order'] = i + 1
        for i in range(len(income_com_amt_list)):
            self.df.loc[self.df['opponent_name'] == income_com_amt_list[i], 'income_amt_order'] = i + 1
        for i in range(len(expense_per_amt_list)):
            self.df.loc[self.df['opponent_name'] == expense_per_amt_list[i], 'expense_amt_order'] = i + 1
        for i in range(len(expense_com_amt_list)):
            self.df.loc[self.df['opponent_name'] == expense_com_amt_list[i], 'expense_amt_order'] = i + 1

    def save_raw_data(self):
        # 原始数据列名
        col_list = ['trans_time', 'opponent_name', 'trans_amt', 'account_balance',
                    'currency', 'opponent_account_no', 'opponent_account_bank', 'trans_channel',
                    'trans_type', 'trans_use', 'remark']
        # account_id = self.df['account_id'].max()
        for row in self.df.itertuples():
            temp_dict = dict()
            # trans_flow表中的id
            temp_dict['flow_id'] = getattr(row, 'id')
            # 当前所有标签表中的account_id取最新录入的一笔流水的account_id
            temp_dict['account_id'] = self.account_id
            # 外部流水报告请求编号
            temp_dict['report_req_no'] = self.report_req_no
            # 其他trans_flow中的字段
            for col in col_list:
                temp_dict[col] = getattr(row, col)
            trans_amt = temp_dict['trans_amt']
            temp_dict['trans_date'] = temp_dict['trans_time'].date()
            temp_dict['trans_time'] = datetime.datetime.strftime(temp_dict['trans_time'], '%H:%M:%S')
            # 交易对手类型
            op_name = temp_dict['opponent_name']
            if hasattr(row, 'opponent_type') and pd.notnull(getattr(row, 'opponent_type')):
                temp_dict['opponent_type'] = getattr(row, 'opponent_type')
            # 手机号
            remark = temp_dict['remark']
            temp_dict['remark_type'] = remark
            remark_num = re.sub(r'[^\u4e00-\u9fa5\d+]', '', remark)
            phone = re.search(r'(?<!\d)1[3-9]\d{9}(?!\d)|(?<=\D86)1[3-9]\d{9}(?!\d)', remark_num)
            if phone is not None:
                temp_dict['phone'] = phone.group(0)
            # 与该账户名的关系
            if self.relation_dict.__contains__(op_name):
                temp_dict['relationship'] = self.relation_dict[op_name]
            # 将合并列拉出来
            concat_str = getattr(row, 'concat_str')
            no_channel_str = op_name + ';' + remark + ';' + temp_dict['trans_use'] + ';' + temp_dict['trans_type']
            # 是否为理财
            if re.search(r'(理财|钱生钱|余额宝|零钱通|招财盈|宜人财富)', concat_str):
                temp_dict['is_financing'] = 1
            # 是否结息
            if hasattr(row, 'is_interest') and pd.notnull(getattr(row, 'is_interest')):
                temp_dict['is_interest'] = 1
            # 贷款类型
            if hasattr(row, 'loan_type') and pd.notnull(getattr(row, 'loan_type')):
                temp_dict['loan_type'] = getattr(row, 'loan_type')
            # 是否还款
            if hasattr(row, 'is_repay') and pd.notnull(getattr(row, 'is_repay')):
                temp_dict['is_repay'] = 1
            # 是否发生在结息还款前一周
            if hasattr(row, 'is_before_interest_repay') and pd.notnull(getattr(row, 'is_before_interest_repay')):
                temp_dict['is_before_interest_repay'] = 1
            # 异常交易类型
            unusual_type = []
            if re.search(r'(彩票|博彩|夜总会|ktv)', concat_str):
                unusual_type.append('博彩娱乐')
            if re.search(r'典当', concat_str):
                unusual_type.append('典当')
            if re.search(r'(法院|律师事务所|检察院|诉讼|律师费|开庭公告|鉴定|保全|判决公告|执行|上诉|执.*号|号.*执)', concat_str):
                unusual_type.append('案件纠纷')
            if re.search(r'(公安|保释)', concat_str) is not None and re.search(r'交通', concat_str) is None:
                unusual_type.append('公安')
            if re.search(r'(保险经纪|保险代理|保险.*理赔|理赔.*保险|赔款)', concat_str):
                unusual_type.append('保险理赔')
            if re.search(r'逾期', concat_str):
                unusual_type.append('逾期')
            if re.search(r'(证券|银证转账|基金)', concat_str):
                unusual_type.append('股票投机')
            if ('21:00:00' <= temp_dict['trans_time'] <= '23:59:59' or
                '00:00:01' <= temp_dict['trans_time'] <= '04:00:00') and \
                    re.search(r'(ATM|atm)', op_name) is not None and trans_amt < 0 and trans_amt % 100 == 0:
                unusual_type.append('夜间不良交易')
            if '00:00:01' <= temp_dict['trans_time'] <= '04:00:00' and '夜间不良交易' not in unusual_type:
                unusual_type.append('夜间交易')
            if trans_amt < 0 and re.search(r'(医院|药房|医疗|门诊|急诊|住院|医药|寿险)', concat_str):
                unusual_type.append('医院')
            if trans_amt > 0 and re.search(r'投资', no_channel_str):
                unusual_type.append('收购')
            if (trans_amt < 0 and re.search(r'投资', no_channel_str)) or \
                    (trans_amt > 0 and re.search(r'(分红|退股)', no_channel_str)):
                unusual_type.append('对外投资')
            if trans_amt > 0 and re.search(r'(购车|购房|首付|车款|房款)', concat_str) is not None and \
                    re.search(r'评估', concat_str) is None:
                unusual_type.append('变现')
            if (trans_amt < 0 and '预收' in no_channel_str) or (trans_amt > 0 and '预付' in no_channel_str):
                unusual_type.append('预收款')
            if trans_amt < 0 and re.search(r'(分红|退股|分润)', no_channel_str):
                unusual_type.append('分红退股')
            if hasattr(row, 'unstable') and pd.notnull(getattr(row, 'unstable')):
                unusual_type.append('家庭不稳定')
            if hasattr(row, 'big_in_out') and pd.notnull(getattr(row, 'big_in_out')):
                unusual_type.append('整进整出')
            if hasattr(row, 'fast_in_out') and pd.notnull(getattr(row, 'fast_in_out')):
                unusual_type.append('快进快出')
            if hasattr(row, 'accidental_big') and pd.notnull(getattr(row, 'accidental_big')):
                unusual_type.append('偶发大额')
            if temp_dict.__contains__('loan_type') and temp_dict['loan_type'] == '民间借贷':
                unusual_type.append('民间借贷')
            if temp_dict.__contains__('is_financing'):
                unusual_type.append('理财')
            if temp_dict.__contains__('loan_type') and temp_dict['loan_type'] == '担保' and \
                    re.search(r'(还本|还息|还款|还贷|本金|利息)', concat_str) is not None:
                unusual_type.append('担保异常')
            if temp_dict.__contains__('opponent_type') and temp_dict['opponent_type'] == 2 and \
                    re.search(r'不良资产', op_name) is not None and \
                    re.search(r'(替.+还款|还.+借款)', remark) is not None:
                unusual_type.append('代偿')
            if len(unusual_type) > 0:
                temp_dict['unusual_trans_type'] = ';'.join(unusual_type)
            # 是否敏感交易标签
            if temp_dict.__contains__('loan_type') or temp_dict.__contains__('unusual_trans_type'):
                temp_dict['is_sensitive'] = 1
            # 成本支出类别标签
            if not (temp_dict.__contains__('relationship') or temp_dict.__contains__('unusual_trans_type')) and \
                    trans_amt < 0:
                if re.search(r'(工资|奖金|年终奖|差旅费|报销|福利费|慰问|公共缴费)', no_channel_str):
                    temp_dict['cost_type'] = '工资'
                elif re.search(r'(电费|水费|水电费|煤气|燃气)', no_channel_str):
                    temp_dict['cost_type'] = '水电'
                elif re.search(r'税', no_channel_str):
                    temp_dict['cost_type'] = '税费'
                elif re.search(r'(房租|租金)', no_channel_str):
                    temp_dict['cost_type'] = '房租'
                elif re.search(r'(保险|保费|维修费)', no_channel_str):
                    temp_dict['cost_type'] = '保险'
                elif re.search(r'(分期|到期|贷款扣款|正常还款|约定还款|扣贷|贷还款|自动收利|贷款还本|贷款还息|收回贷款本息|'
                               r'归还贷款|贷款利息|归还个贷|银行利息|利息收入|信用卡.*还款)', no_channel_str):
                    temp_dict['cost_type'] = '到期贷款'
            # 进账笔数排名标签
            if hasattr(row, 'income_cnt_order') and pd.notnull(getattr(row, 'income_cnt_order')):
                temp_dict['income_cnt_order'] = getattr(row, 'income_cnt_order')
            # 出账笔数排名标签
            if hasattr(row, 'expense_cnt_order') and pd.notnull(getattr(row, 'expense_cnt_order')):
                temp_dict['expense_cnt_order'] = getattr(row, 'expense_cnt_order')
            # 进账金额排名标签
            if hasattr(row, 'income_amt_order') and pd.notnull(getattr(row, 'income_amt_order')):
                temp_dict['income_amt_order'] = getattr(row, 'income_amt_order')
            # 出账金额排名标签
            if hasattr(row, 'expense_amt_order') and pd.notnull(getattr(row, 'expense_amt_order')):
                temp_dict['expense_amt_order'] = getattr(row, 'expense_amt_order')
            temp_dict['create_time'] = self.create_time
            temp_dict['update_time'] = self.create_time
            # 将标签表数据落到数据库
            label_role = transform_class_str(temp_dict, 'TransFlowPortrait')
            self.label_list.append(label_role)
