from portrait.transflow.single_account_portrait.trans_flow import transform_class_str
from logger.logger_util import LoggerUtil
from util.mysql_reader import sql_to_df
import pandas as pd
import datetime

logger = LoggerUtil().logger(__name__)


class TransFlowRawData:
    """
    将流水账户表和流水数据表落库
    author:汪腾飞
    created_time:20200706
    updated_time_v1:20200707新增是否有新增数据字段,若有则有所有后续操作,若无,则无后续操作
    updated_time_v2:20200818添加commit的事务性,若发生错误则全部不提交
    """

    def __init__(self, sql_db, param, title_param, resp):
        self.df = None
        self.db = sql_db
        self.param = param
        self.title_param = title_param
        self.raw_list = []
        self.label_list = []
        self.create_time = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')
        # 是否有新增数据,若为True,则需要运行save_raw_data,若无则不需要
        self.new_data = True
        self.resp = resp

    def remove_duplicate_data(self, data):
        """
        从数据库里面找到对应银行账号最后一次上传的记录,查找最后一次上传记录与本次记录的时间是否有交集
        如果有则从本次记录中删除上次记录最后时间之前的所有记录,即将本次时间重复的部分删除后再上传
        todo 若补全以前缺失的流水如何处理
        :return: 
        """
        sql = """select * from trans_flow where account_id = (select id from trans_account where account_name='%s' and 
            id_card_no='%s' and account_no='%s' order by id desc limit 1) order by id desc""" % \
              (self.param.get('cusName'), self.param.get('idNo'), self.param.get('bankAccount'))
        df = sql_to_df(sql)
        df['trans_time'] = pd.to_datetime(df['trans_time'])

        length = min(df.shape[0], 10)
        if length == 0:
            return data
        remove_index = -1
        for i in range(length):
            temp_trans_time = df.loc[i, 'trans_time']
            temp_trans_amt = df.loc[i, 'trans_amt']
            temp_trans_balance = df.loc[i, 'account_balance']
            temp_df = data.loc[(data['trans_time'] == temp_trans_time) & 
                               (data['trans_amt'] == temp_trans_amt) &
                               (data['account_balance'] == temp_trans_balance)]
            if len(temp_df) > 0:
                remove_index = temp_df.index.to_list()[0]
                break
        if remove_index != -1:
            data = data[data.index > remove_index]
            data.reset_index(drop=True, inplace=True)
        if data.shape[0] == 0:
            self.new_data = False
            logger.info('录入数据已存在于数据库,不再重复录入,cus_name: %s,   id_card_no: %s,     time:%s' %
                        (self.param.get('cusName'), self.param.get('idNo'), self.create_time))
        return data

    def _save_account_data(self):
        """
        将处理过后的流水数据的基本信息存入trans_account表,并将得到的account_id传入trans_flow表
        :return:
        """
        min_trans_time = self.df['trans_time'].min()
        max_trans_time = self.df['trans_time'].max()
        min_trans_time = datetime.datetime.strftime(min_trans_time, '%Y-%m-%d %H:%M:%S')
        max_trans_time = datetime.datetime.strftime(max_trans_time, '%Y-%m-%d %H:%M:%S')
        temp_dict = dict()
        temp_dict['account_name'] = self.param.get('cusName')
        temp_dict['id_card_no'] = self.param.get('idNo')
        temp_dict['id_type'] = self.param.get('idType')
        temp_dict['bank'] = self.title_param.get('bank', self.param.get('bankName'))
        temp_dict['account_no'] = self.param.get('bankAccount')
        temp_dict['start_time'] = self.title_param.get('start_date', min_trans_time)
        temp_dict['end_time'] = self.title_param.get('end_time', max_trans_time)
        temp_dict['trans_flow_type'] = 1 if self.param.get('cusType') == 'PERSONAL' else 2
        temp_dict['update_time'] = self.create_time
        temp_dict['create_time'] = self.create_time
        temp_dict['account_state'] = 1
        role = transform_class_str(temp_dict, 'TransAccount')
        self.raw_list.append(role)
        self.db.session.add(role)
        self.db.session.flush()
        return role.id

    def save_raw_data(self):
        account_id = self._save_account_data()
        # 原始数据列名
        col_list = ['trans_time', 'opponent_name', 'trans_amt', 'account_balance', 'currency', 'opponent_account_no',
                    'opponent_account_bank', 'trans_channel', 'trans_type', 'trans_use', 'remark']
        for row in self.df.itertuples():
            temp_dict = dict()
            temp_dict['account_id'] = account_id
            temp_dict['out_req_no'] = self.param.get('outReqNo')
            for col in col_list:
                temp_dict[col] = getattr(row, col)
            temp_dict['create_time'] = self.create_time
            temp_dict['update_time'] = self.create_time
            # 将原始数据落库
            role = transform_class_str(temp_dict, 'TransFlow')
            self.raw_list.append(role)
        self.db.session.add_all(self.raw_list)
        try:
            self.db.session.commit()
        except Exception as e:
            self.db.session.rollback()
            self.resp['resCode'] = '1'
            self.resp['resMsg'] = '失败'
            logger.info('导入数据库失败,失败原因:%s' % str(e))
            self.resp['data']['warningMsg'] = ['导入数据库失败']
