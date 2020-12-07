import pandas as pd

from logger.logger_util import LoggerUtil
from mapping.tranformer import Transformer

logger = LoggerUtil().logger(__name__)


class BusinessInfo(Transformer):

    def __init__(self):
        super().__init__()
        self.variables = {
            'bus_total_accounts_payable_debt_amt': None,  # 企业应付账款总负债
            'operating_cycle': 0,  # 营业周期
            'main_business_running_years': None,  # 主营企业经营年限
            'bus_total_assets_amt': None,  # 企业总资产
            'total_currency_assets_amt': None,  # 个人_企业货币资金总资产
            'quick_debt_to_assets_ratio': None,  # 速动比率
            'sale_to_loan_ratio': None,  # 销贷比
            'bus_industry_2nd_cnt': None,  # 二类行业数
            'per_guar_debt_balance': None  # 个人对外担保总负债余额
        }
        self.full_msg = None
        self.per_asset_info = None
        self.per_debt_info = None
        self.com_busi_info = None
        self.com_sale_info = None
        self.com_asset_info = None
        self.com_debt_info = None

    def all_variables(self):
        per_total_debt_amt = 0
        per_total_bank_debt_amt = 0
        if self.per_debt_info is not None and self.per_debt_info.shape[0] > 0:
            self.per_debt_info['indiv_debt_amt'] = self.per_debt_info.apply(
                lambda x: x['indiv_debt_amt'] if x['indiv_debt_typ'] != '4' else x['indiv_debt_amt'] / 2, axis=1)
            self.per_debt_info['indiv_debt_amt_bal'] = self.per_debt_info.apply(
                lambda x: x['indiv_debt_amt_bal'] if x['indiv_debt_typ'] != '4' else x['indiv_debt_amt_bal'] / 2,
                axis=1)
            self.variables['per_guar_debt_balance'] = \
                self.per_debt_info[self.per_debt_info['indiv_debt_typ'] == '4']['indiv_debt_amt_bal'].sum()

            per_total_debt_amt = self.per_debt_info['indiv_debt_amt_bal'].sum()
            per_total_bank_debt_amt = self.per_debt_info[
                self.per_debt_info['indiv_debt_typ'].isin(['2', '3', '5'])]['indiv_debt_amt_bal'].sum()
        
        per_currency_assets_amt = 0
        per_total_flow_assets_amt = 0
        if self.per_asset_info is not None and self.per_asset_info.shape[0] > 0:
            self.per_asset_info['indiv_ass_val'] = self.per_asset_info.apply(
                lambda x: x['s_avg_daily'] if x['indiv_ass_type'] == '05' and pd.notna(x['s_avg_daily'])
                else x['indiv_ass_val'], axis=1)
            per_currency_assets_amt = self.per_asset_info[
                self.per_asset_info['indiv_ass_type'] == "05"]['indiv_ass_val'].sum()
            per_total_flow_assets_amt = self.per_asset_info[
                self.per_asset_info['indiv_ass_type'].isin(['05', '06', '07'])]['indiv_ass_val'].sum()

        if self.com_busi_info is not None and self.com_busi_info.shape[0] > 0:
            self.variables['bus_industry_2nd_cnt'] = \
                self.com_busi_info[(pd.notna(self.com_busi_info['cll_typ'])) & 
                                   (self.com_busi_info['cll_typ'] != '')]['cll_typ'].apply(lambda x: x[:3]).nunique()
            
            self.com_busi_info['opera_year'].replace('', 0, inplace=True)

            last_total_sale_amt = 0
            last_total_profit_amt = 0
            if self.com_sale_info is not None and self.com_sale_info.shape[0] > 0:
                temp_sale_info = pd.merge(self.com_sale_info, self.com_busi_info[['serno', 'stock_perc']],
                                          how='left', left_on='bus_info_serno', right_on='serno')
                temp_sale_info['stock_perc'] = temp_sale_info['stock_perc'].fillna(0)
                temp_sale_info['interest_fee'] = temp_sale_info['interest_fee'].fillna(0)
                try:
                    opera_year = self.com_busi_info[self.com_busi_info['serno'].isin(
                        self.com_sale_info['bus_info_serno'].to_list())]['opera_year'].max()
                except TypeError:
                    opera_year = -999
                self.variables['main_business_running_years'] = opera_year

                last_total_sale_amt = sum(temp_sale_info['last_sale_total'] * temp_sale_info['stock_perc'])
                last_total_profit_amt = sum(temp_sale_info['last_gross_total'] * temp_sale_info['stock_perc'])

            bus_total_flow_assets_amt = 0
            bus_total_stock_assets_amt = 0
            bus_total_receivables_assets_amt = 0
            if self.com_asset_info is not None and self.com_asset_info.shape[0] > 0:
                temp_asset_info = pd.merge(self.com_asset_info, self.com_busi_info[['serno', 'stock_perc']],
                                           how='left', left_on='bus_info_serno', right_on='serno')
                temp_asset_info['stock_perc'] = temp_asset_info['stock_perc'].fillna(0)
                temp_asset_info['cost_perc'] = temp_asset_info['cost'] * temp_asset_info['stock_perc']
                self.variables['bus_total_assets_amt'] = temp_asset_info['cost_perc'].sum()
                bus_total_busi_currency_assets_amt = \
                    temp_asset_info[temp_asset_info['asset_typ'] == '1']['cost_perc'].sum()
                self.variables['total_currency_assets_amt'] = \
                    per_currency_assets_amt + bus_total_busi_currency_assets_amt

                bus_total_stock_assets_amt = temp_asset_info[temp_asset_info['asset_typ'] == '4']['cost_perc'].sum()
                bus_total_receivables_assets_amt = \
                    temp_asset_info[temp_asset_info['asset_typ'] == '2']['cost_perc'].sum()
                bus_total_flow_assets_amt = temp_asset_info[
                    temp_asset_info['asset_typ'].isin(['1', '2', '3', '4', '5', '6'])]['cost_perc'].sum()

            bus_total_debt_amt = 0
            bus_total_bank_debt_amt = 0
            if self.com_debt_info is not None and self.com_debt_info.shape[0] > 0:
                temp_debt_info = pd.merge(self.com_debt_info, self.com_busi_info[['serno', 'stock_perc']],
                                          how='left', left_on='bus_info_serno', right_on='serno')
                temp_debt_info['stock_perc'] = temp_debt_info['stock_perc'].fillna(0)
                temp_debt_info['advan_amt'] = temp_debt_info.apply(
                    lambda x: x['advan_amt'] if x['debt_typ'] != '5' else x['advan_amt'] / 2, axis=1)
                temp_debt_info['cost_perc'] = temp_debt_info['advan_amt'] * temp_debt_info['stock_perc']

                self.variables['bus_total_accounts_payable_debt_amt'] = temp_debt_info[
                    temp_debt_info['debt_typ'] == '2']['cost_perc'].sum()

                bus_total_debt_amt = temp_debt_info['cost_perc'].sum()
                bus_total_bank_debt_amt = temp_debt_info[temp_debt_info['debt_typ'] == '1']['cost_perc'].sum()

            inventory_turnover_ratio = (last_total_sale_amt - last_total_profit_amt) / bus_total_stock_assets_amt \
                if bus_total_stock_assets_amt > 0 else -999
            receivables_turnover_ratio = last_total_sale_amt / bus_total_receivables_assets_amt \
                if bus_total_receivables_assets_amt > 0 else -999
            cash_conversion_cycle = 0
            cash_conversion_cycle += 365 / inventory_turnover_ratio if inventory_turnover_ratio > 0 else 0
            cash_conversion_cycle += 365 / receivables_turnover_ratio if receivables_turnover_ratio > 0 else 0
            self.variables['operating_cycle'] = cash_conversion_cycle

            self.variables['quick_debt_to_assets_ratio'] = \
                (per_total_debt_amt + bus_total_debt_amt) / \
                (bus_total_flow_assets_amt + per_total_flow_assets_amt - bus_total_stock_assets_amt) \
                if bus_total_flow_assets_amt + per_total_flow_assets_amt - bus_total_stock_assets_amt != 0 else -999
            self.variables['sale_to_loan_ratio'] = \
                (per_total_bank_debt_amt + bus_total_bank_debt_amt) / last_total_sale_amt \
                if last_total_sale_amt != 0 else -999

    def transform(self):
        per_msg = self.full_msg.get('ruskSubject')
        com_msg = self.full_msg.get('passthroughMsg')
        if per_msg is None or com_msg is None:
            return
        per_passthrough_msg = per_msg.get('passthroughMsg')
        if per_passthrough_msg is None:
            return
        self.per_asset_info = pd.DataFrame(per_passthrough_msg.get('iqp_indiv_ass'))
        self.per_debt_info = pd.DataFrame(per_passthrough_msg.get('iqp_indiv_fam_lby'))
        self.com_busi_info = pd.DataFrame(com_msg.get('iqp_busi_info'))
        self.com_sale_info = pd.DataFrame(com_msg.get('iqp_sale_info'))
        self.com_asset_info = pd.DataFrame(com_msg.get('iqp_asset_info'))
        self.com_debt_info = pd.DataFrame(com_msg.get('iqp_debt_info'))
        self.all_variables()
