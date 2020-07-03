import pandas as pd
import re
from mapping.trans_module_processor import TransModuleProcessor

class BasicInfoProcessor(TransModuleProcessor):


    def clean_oppo_type(self):


        def end_string( x, list):
            for i in list:
                if x.endswith(i):
                    return 2

        def not_end_string( x, list):
            for i in list:
                if not x.endswith(i):
                    return 1

        def opponent_type2(flow_df):

            oppo_type_2 = flow_df[['id','opponent_name']]
            oppo_type_2['length'] = oppo_type_2['opponent_name'].apply(len)
            oppo_type_2 = oppo_type_2[oppo_type_2['length']>6]
            oppo_type_2['opponent_type'] = oppo_type_2['opponent_name'].apply( lambda x: end_string( x, ['厂','店','公司','经营部']))

            return oppo_type_2[pd.notnull(oppo_type_2['opponent_type'])][['id','opponent_type']]

        def opponent_type1(flow_df):
            oppo_type_1 = flow_df[pd.isnull(flow_df['opponent_type'])][['id','opponent_name']]
            oppo_type_1['length'] = oppo_type_1['opponent_name'].apply(len)
            oppo_type_1 = oppo_type_1[oppo_type_2['length'] < 10 ]
            oppo_type_1['cleaned_name'] = oppo_type_1['opponent_name'].apply(lambda x: x.replace('支付宝转账', ''))
            oppo_type_1 = oppo_type_1[~oppo_type_1['cleaned_name'].str.contains(r'[a-zA-Z0-9]')]

            dict_ZFB = {
                ' ': '',
                '-': '',
                '－': '',
                '，': '',
                '：': '',
                '支付宝': '',
            }
            for i in dict_ZFB:
                oppo_type_1['cleaned_name'] = oppo_type_1['cleaned_name'].apply(lambda x: x.replace(i, dict_ZFB[i])
                                            if x.startswith('支付宝') else x)
            oppo_type_1['cleaned_name'] = oppo_type_1['cleaned_name'].apply(lambda x: x.split(' ')[-1]
                                            if x.startswith('转账') else x)

            oppo_type_1['length'] = oppo_type_1['cleaned_name'].apply(len)
            oppo_type_1 = oppo_type_1[(oppo_type_1['length'] < 7) & (oppo_type_1['length'] > 1)]

            regex1 = re.compile(r'[-, ,\t,：:....()（）]')
            oppo_type_1['cleaned_name'] = oppo_type_1['cleaned_name'].str.replace(regex1, '')
            oppo_type_1 = oppo_type_1[~oppo_type_1['cleaned_name'].str.contains('转|消费|其他|自定义|理财|缴费|还款|充值|商户|天猫|租金|淘宝|备用|撤销|自取|支出')]
            oppo_type_1['length'] = oppo_type_1['cleaned_name'].apply(len)
            oppo_type_1 = oppo_type_1[(oppo_type_1['length']>1)&(oppo_type_1['length']<4)]
            oppo_type_1['opponent_type'] = oppo_type_1['cleaned_name'].apply(lambda x: not_end_string(x, ['费', '税', '款']))

            return oppo_type_1[pd.notnull(oppo_type_1['opponent_type'])][['id', 'opponent_type']]

        flow_df = self.cached_data['trans_flow']
        oppo_type_2 = opponent_type2(flow_df)
        flow_df2 = pd.merge(flow_df[['id','opponent_name']], oppo_type_2[['id', 'opponent_type']], how='left', on='id')
        oppo_type_1 = opponent_type1(flow_df2)
        oppo_type = pd.concat([oppo_type_2,oppo_type_1])

        flow_df = pd.merge( flow_df, oppo_type , how = 'left' , on = 'id')

    def get_loan_type(self):

        pass

