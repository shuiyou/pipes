# -*- coding: utf-8 -*-
import importlib

# from app import logger
import numpy

from exceptions import ServerException
from logger.logger_util import LoggerUtil
from mapping.tranformer import Transformer, numpy_to_int

logger = LoggerUtil().logger(__name__)

# 标识决策引擎交互完成。
STRATEGE_DONE = 'fffff'


def _get_codes_by_product_code(product_code):
    # TODO: 需要根据产品编码配置对应的code
    return ['10001', '13001', '12001', '17001', '07001', '09001']

def extension_variables(variables):
    """
    构建展示变量
    :param variables:
    :return:
    """
    anti_apply_bank_7d = variables['anti_apply_bank_7d']
    net_apply_bank_7d = variables['net_apply_bank_7d']
    oth_loan_apply_bank_7d = variables['oth_loan_apply_bank_7d']
    apply_bank_7d_arr = [anti_apply_bank_7d, net_apply_bank_7d, oth_loan_apply_bank_7d]
    anti_apply_bank_1m = variables['anti_apply_bank_1m']
    net_apply_bank_1m = variables['net_apply_bank_1m']
    oth_loan_apply_bank_1m = variables['oth_loan_apply_bank_1m']
    apply_bank_1m_arr = [anti_apply_bank_1m, net_apply_bank_1m, oth_loan_apply_bank_1m]
    anti_apply_bank_3m = variables['anti_apply_bank_3m']
    net_apply_bank_3m = variables['net_apply_bank_3m']
    oth_loan_apply_bank_3m = variables['oth_loan_apply_bank_3m']
    apply_bank_3m_arr = [anti_apply_bank_3m, net_apply_bank_3m, oth_loan_apply_bank_3m]
    apply_sloan_7d_arr = [variables['anti_apply_sloan_7d'], variables['net_apply_sloan_7d'],
                          variables['oth_loan_apply_sloan_7d']]
    apply_sloan_1m_arr = [variables['anti_apply_sloan_1m'], variables['net_apply_sloan_1m'],
                          variables['oth_loan_apply_sloan_1m']]
    apply_sloan_3m_arr = [variables['anti_apply_sloan_3m'], variables['net_apply_sloan_3m'],
                          variables['oth_loan_apply_sloan_3m']]
    apply_p2p_7d_arr = [variables['anti_apply_p2p_7d'], variables['net_apply_p2p_7d'],
                        variables['oth_loan_apply_p2p_7d']]
    apply_p2p_1m_arr = [variables['anti_apply_p2p_1m'], variables['net_apply_p2p_1m'],
                        variables['oth_loan_apply_p2p_1m']]
    apply_p2p_3m_arr = [variables['anti_apply_p2p_3m'], variables['net_apply_p2p_3m'],
                        variables['oth_loan_apply_p2p_3m']]
    apply_confin_7d_arr = [variables['anti_apply_confin_7d'], variables['net_apply_confin_7d'],
                           variables['oth_loan_apply_confin_7d']]
    apply_confin_1m_arr = [variables['anti_apply_confin_1m'], variables['net_apply_confin_1m'],
                           variables['oth_loan_apply_confin_1m']]
    apply_confin_3m_arr = [variables['anti_apply_confin_3m'], variables['net_apply_confin_3m'],
                           variables['oth_loan_apply_confin_3m']]
    apply_other_7d_arr = [variables['anti_apply_other_7d'], variables['net_apply_other_7d'],
                          variables['oth_loan_apply_other_7d']]
    apply_other_1m_arr = [variables['anti_apply_other_1m'], variables['net_apply_other_1m'],
                          variables['oth_loan_apply_other_1m']]
    apply_other_3m_arr = [variables['anti_apply_other_3m'], variables['net_apply_other_3m'],
                          variables['oth_loan_apply_other_3m']]
    apply_bank_6m_arr = [variables['oth_loan_apply_bank_6m'], variables['net_apply_bank_6m']]
    apply_bank_12m_arr = [variables['oth_loan_apply_bank_12m'], variables['net_apply_bank_12m']]
    apply_bank_his_arr = [variables['oth_loan_apply_bank_his'], variables['net_apply_bank_his']]
    apply_sloan_6m_arr = [variables['oth_loan_apply_sloan_6m'], variables['net_apply_sloan_6m']]
    apply_sloan_12m_arr = [variables['oth_loan_apply_sloan_12m'], variables['net_apply_sloan_12m']]
    apply_sloan_his_arr = [variables['oth_loan_apply_sloan_his'], variables['net_apply_sloan_his']]
    apply_p2p_6m_arr = [variables['oth_loan_apply_p2p_6m'], variables['net_apply_p2p_6m']]
    apply_p2p_12m_arr = [variables['oth_loan_apply_p2p_12m'], variables['net_apply_p2p_12m']]
    apply_p2p_his_arr = [variables['oth_loan_apply_p2p_his'], variables['net_apply_p2p_his']]
    apply_confin_6m_arr = [variables['oth_loan_apply_confin_6m'], variables['net_apply_confin_6m']]
    apply_confin_12m_arr = [variables['oth_loan_apply_confin_12m'], variables['net_apply_confin_12m']]
    apply_confin_his_arr = [variables['oth_loan_apply_confin_his'], variables['net_apply_confin_his']]
    apply_other_6m_arr = [variables['oth_loan_apply_other_6m'], variables['net_apply_other_6m']]
    apply_other_12m_arr = [variables['oth_loan_apply_other_12m'], variables['net_apply_other_12m']]
    apply_other_his_arr = [variables['oth_loan_apply_other_his'], variables['net_apply_other_his']]
    extension = {
        'apply_bank_7d': round_max(apply_bank_7d_arr),
        'apply_bank_1m': round_max(apply_bank_1m_arr),
        'apply_bank_3m': round_max(apply_bank_3m_arr),
        'apply_sloan_7d': round_max(apply_sloan_7d_arr),
        'apply_sloan_1m': round_max(apply_sloan_1m_arr),
        'apply_sloan_3m': round_max(apply_sloan_3m_arr),
        'apply_p2p_7d': round_max(apply_p2p_7d_arr),
        'apply_p2p_1m': round_max(apply_p2p_1m_arr),
        'apply_p2p_3m': round_max(apply_p2p_3m_arr),
        'apply_confin_7d': round_max(apply_confin_7d_arr),
        'apply_confin_1m': round_max(apply_confin_1m_arr),
        'apply_confin_3m': round_max(apply_confin_3m_arr),
        'apply_other_7d': round_max(apply_other_7d_arr),
        'apply_other_1m': round_max(apply_other_1m_arr),
        'apply_other_3m': round_max(apply_other_3m_arr),
        'apply_bank_6m': round_max(apply_bank_6m_arr),
        'apply_bank_12m': round_max(apply_bank_12m_arr),
        'apply_bank_his': round_max(apply_bank_his_arr),
        'apply_sloan_6m': round_max(apply_sloan_6m_arr),
        'apply_sloan_12m': round_max(apply_sloan_12m_arr),
        'apply_sloan_his': round_max(apply_sloan_his_arr),
        'apply_p2p_6m': round_max(apply_p2p_6m_arr),
        'apply_p2p_12m': round_max(apply_p2p_12m_arr),
        'apply_p2p_his': round_max(apply_p2p_his_arr),
        'apply_confin_6m': round_max(apply_confin_6m_arr),
        'apply_confin_12m': round_max(apply_confin_12m_arr),
        'apply_confin_his': round_max(apply_confin_his_arr),
        'apply_other_6m': round_max(apply_other_6m_arr),
        'apply_other_12m': round_max(apply_other_12m_arr),
        'apply_other_his': round_max(apply_other_his_arr)
    }
    variables.update(extension)


def round_max(max_arr, median_arr=None, ratio=0.3):
    if median_arr is None:
        median_arr = max_arr
    return round(
        numpy.amax(max_arr) + ratio * numpy.median(median_arr),
        0
    )


def translate_for_report_detail(product_code, user_name=None, id_card_no=None, phone=None, user_type=None):
    """
    处理后的结果作为决策需要的变量。
    :return: 一个dict对象包含产品所需要的变量
    """

    variables = {}

    try:
        codes = _get_codes_by_product_code(product_code)
        for c in codes:
            trans = get_transformer(c)
            trans_result = trans.run(user_name=user_name,
                                     id_card_no=id_card_no,
                                     phone=phone,
                                     user_type=user_type)
            variables.update(trans_result['variables'])
    except Exception as err:
        logger.error(">>> translate error: " + str(err))
        raise ServerException(code=500, description=str(err))

    extension_variables(variables)
    # 转换类型，这样解决tojson的问题
    numpy_to_int(variables)
    return {
        'variables': variables
    }


def get_transformer(code) -> Transformer:
    """
    根据code构建对应的转换对象
    :param code:
    :return:
    """
    try:
        model = importlib.import_module("view.v" + str(code))
        api_class = getattr(model, "V" + str(code))
        api_instance = api_class()
        return api_instance
    except ModuleNotFoundError as err:
        logger.error(str(err))
        return Transformer()
