import json
import traceback

import pandas as pd
from numpy import int64
from jsonpath import jsonpath

from logger.logger_util import LoggerUtil

logger = LoggerUtil().logger(__name__)


def to_string(obj):
    if obj is None:
        return ''
    return str(obj)


def format_timestamp(obj):
    if obj is not None and pd.notna(obj):
        return obj.strftime('%Y-%m-%d')
    else:
        return ''


def exception(describe):
    def robust(actual_do):
        def add_robust(*args, **keyargs):
            try:
                return actual_do(*args, **keyargs)
            except Exception as e:
                logger.error(describe)
                logger.error(traceback.format_exc())

        return add_robust

    return robust


def replace_nan(values):
    v_list = [x if pd.notna(x) else 0 for x in values]
    result = []
    for v in v_list:
        if isinstance(v, int64):
            result.append(int(str(v)))
        else:
            result.append(v)

    return result


def get_query_data(msg, query_user_type, query_strategy):
    query_data_list = jsonpath(msg, '$..queryData[*]')
    resp = []
    for query_data in query_data_list:
        name = query_data.get("name")
        idno = query_data.get("idno")
        user_type = query_data.get("userType")
        strategy = query_data.get("extraParam")['strategy']
        education = query_data.get("extraParam")['education']
        mar_status = query_data.get('extraParam')['marryState']
        priority = query_data.get('extraParam')['priority']
        phone = query_data.get("phone")
        if pd.notna(query_user_type) and user_type == query_user_type and strategy == query_strategy:
            resp_dict = {"name": name, "id_card_no": idno, 'phone': phone,
                         'education': education, 'marry_state': mar_status, 'priority':priority}
            resp.append(resp_dict)
        if pd.isna(query_user_type) and strategy == query_strategy:
            resp_dict = {"name": name, "id_card_no": idno}
            resp.append(resp_dict)
    return resp


def get_all_related_company(msg):
    query_data_list = jsonpath(msg, '$..queryData[*]')
    per_type = dict()
    resp = dict()
    for query_data in query_data_list:
        name = query_data.get("name")
        idno = query_data.get("idno")
        user_type = query_data.get("userType")
        base_type = query_data.get("baseType")
        strategy = query_data.get("extraParam")['strategy']
        industry = query_data.get("extraParam")['industry']
        if user_type == 'PERSONAL' and strategy == '01':
            resp[idno] = {'name': [name], 'idno': [idno], 'industry': [industry]}
            if base_type == 'U_PERSONAL':
                per_type['main'] = idno
            elif 'SP' in base_type:
                per_type['spouse'] = idno
            elif 'CT' in base_type:
                per_type['controller'] = idno
            # else:
            #     per_type[base_type] = idno
    for query_data in query_data_list:
        name = query_data.get("name")
        idno = query_data.get("idno")
        user_type = query_data.get("userType")
        base_type = query_data.get("baseType")
        strategy = query_data.get("extraParam")['strategy']
        industry = query_data.get("extraParam")['industry']
        temp_code = None
        if user_type == 'COMPANY' and strategy == '01':
            if 'SP' in base_type:
                temp_code = per_type.get('spouse')
            if 'CT' in base_type and temp_code is None:
                temp_code = per_type.get('controller')
            if temp_code is None:
                temp_code = per_type.get('main')
            if temp_code is not None:
                resp[temp_code]['name'].append(name)
                resp[temp_code]['idno'].append(idno)
                resp[temp_code]['industry'].append(industry)
    return resp


def get_industry_risk_level(industry_code):
    if industry_code in ['H623', 'H622', 'H621', 'L727', 'C183', 'C366', 'P821', 'L726', 'H629', 'C231', 'F523',
                         'P829', 'F513', 'O795', 'O794']:
        return "D"
    elif industry_code in ['F525', 'F514', 'E492', 'C223', 'E501', 'N781', 'A014', 'F518', 'C219',
                           'F528', 'E470', 'H612', 'C339', 'L729', 'L724', 'H611', 'A021', 'Q839',
                           'O801', 'F515', 'E502', 'A015', 'A041', 'C175', 'C182', 'C201']:
        return "C"
    elif industry_code in ['G543', 'C203', 'F529', 'O799', 'E489', 'L721', 'F511', 'C331', 'F524',
                           'F517', 'H619', 'F522', 'E499', 'F527', 'C211', 'F526', 'F516', 'L711',
                           'C292', 'F519', 'G582', 'C352', 'C336', 'E481', 'C335', 'C326', 'C338',
                           'C342', 'C348', 'C382', 'C419', 'E503', 'G581', 'G599', 'K702', 'L712',
                           'O811']:
        return "B"
    elif industry_code in ['F521', 'F512', 'C135']:
        return "A"
    else:
        return "暂无风险评级"


def get_industry_risk_tips(industry_code):
    resp_list = []
    if industry_code in ['E501', 'E5010']:
        resp_list.append("1、如果企业规模小、仍运用传统的、主要以体力为支出的运作模式，抗风险能 力较弱。")
        resp_list.append("2、请关注该行业的挂靠和转包、分包现象，以及是否有行业等级资质。")
        resp_list.append("3、请关注隐形负债，该行业垫资多，且多为民营企业，企业发展资金主要依靠自身积累，融资渠道也有限，民间借贷普遍存在。")
        resp_list.append("4、如果应收账款超年营业额70%以上，请关注应收账款质量，存在坏账可能。")
        resp_list.append("5、企业主实际经营年限不足3年，风险较高。如果有实力较好的从事相关行业的经营者给与支持或者合作，可适当降低风险。")
    elif industry_code in ['A041']:
        resp_list.append("1、请关注是否购买行业保险，保险可对冲自然灾害风险。")
        resp_list.append("2、该行业生产周期长，季节性明显，建议将贷款到期日设置在销售旺季。")
        resp_list.append("3、请关注饲料供应是否有稳定的来源及稳定的价格。")
        resp_list.append("4、请关注苗种的来源和质量是否稳定。")
        resp_list.append("5、该行业的现金流及融资能力普遍较弱，建议有增信措施。")
    elif industry_code in ['F528', 'F5281']:
        resp_list.append("1、请关注客户所代理的品牌在当地或者在行业内是否有一定的知名度。")
        resp_list.append("2、请关注客户线下店面所在地的专业市场是否人气不足，整体不景气。")
        resp_list.append("3、如果销售的五金产品种类少及规模小，请谨慎授信。")
        resp_list.append("4、3年内经营场地搬迁2次以上，固定资产保障性较弱的客群，请谨慎授信。")
    elif industry_code in ['E48']:
        resp_list.append("1、如果经营主体资质不足，一般采用挂靠、转包、分包形式施工的，应收账款回款周期较慢。")
        resp_list.append("2、请核实应收账款质量，行业应收账款普遍存在较长账期或者坏账可能。")
        resp_list.append("3、请关注隐形负债，该行业普遍存在应收款质押、设备融资租赁、民间负债多等现象。")
        resp_list.append("4、请关注法律风险，该行业易产生合同纠纷、劳务纠纷、借款纠纷。")
        resp_list.append("5、该行业客户固定资产持有率高，如果借款人或实际控制人固定资产少、保障性弱，请谨慎授信。")
        resp_list.append("6、该行业客户资金调动能力较强，如果客户现金流差、资金调动能力弱，请谨慎授信。")
    elif industry_code in ['H62']:
        resp_list.append("1、请关注客户的资质完备情况，餐饮行业基本资质证件含《营业执照》、《食品经营许可证》、《卫生安全许可证》、《消防安全许可证》或根据各省市实际情况判定。")
        resp_list.append("2、如果营业执照非本人，通过租赁场地经营的，该类客户经营稳定性差。")
        resp_list.append("3、请关注客户从业年限和经营店面开业年限，同一店面经营时间3年以上违约风险会显著降低。")
        resp_list.append("4、请关注店铺实际口碑情况，可关注大众点评等网评差评内容。")
        resp_list.append("5、请关注该行业的核心员工稳定度（管理人员、店长、主厨等）。")
        resp_list.append("6、餐饮行业不建议将装修投资记录资产，并请谨慎评估原始投资资金来源。")
    elif industry_code in ['F5212']:
        resp_list.append("1、请关注客户的资质完备情况，商超行业基本资质证件含《营业执照》、《食品经营许可证》、《消防安全许可证》或根据各省市实际情况判定。")
        resp_list.append("2、请关注公司名下行政处罚是否涉及食品安全问题及货款纠纷等。")
        resp_list.append("3、请关注客户经营地段是否在人口流动集聚的社区及地域。")
        resp_list.append("4、请关注经营超市的品牌是否在经营当地具备一定认可度，尤其关注客户自有品牌的市场认可度。")
        resp_list.append("5、请关注单体商超的实际股份构成，以及企业主其他多元化对外投资情况。")
        resp_list.append("6、如果企业主实际经营商超行业不足5年，请谨慎授信。")
    elif industry_code in ['G543', 'G5430']:
        resp_list.append("1、请关注客户的资质完备情况，物流运输行业基本资质证件含《营业执照》、《道路运输经营许可证》或根据各省市实际情况判定。")
        resp_list.append("2、请关注客户是否购买车辆保险且是否足额，防范交通事故赔偿风险。")
        resp_list.append("3、请核实客户公司名下车辆所有权及融资情况，关注是否有已报废的车辆继续营运的情况。")
        resp_list.append("4、请关注客户近期或者历史有无未结清的交通案件，如果案件涉及金额较大或情节较严重的，请谨慎授信。")
        resp_list.append("5、请核实应收账款质量，行业应收账款普遍存在较长账期或者坏账可能。")
        resp_list.append("6、请关注客户在经营当地固定资产的保障情况，该行业的资产投入较集中在经营性资产上（车辆及应收）。")
    elif industry_code in ['C2438', 'F5245']:
        resp_list.append("1、请关注品牌在经营当地的知名度和市场接受度。")
        resp_list.append("2、请关注隐形负债，该行业内拆借及民间借贷情况普遍存在。")
        resp_list.append("3、请关注多头信贷，该行业普遍存在暗股的情况。")
        resp_list.append("4、该行业存货价值较高、变现能力强，且存货体积较小，存挪比较方便，对于发生风险后的实际处置力有一定隐患，建议有增信措施。")
        resp_list.append("5、请关注国际金价波动对企业经营的影响。")
        resp_list.append("6、如果客户在经营当地无固定资产，请谨慎授信。")
        resp_list.append("7、如果客户为福建籍，请谨慎授信。")
    elif industry_code in ['F5261']:
        resp_list.append("1、请关注隐形负债，该行业车辆融资租赁以及金融机构特殊授信产品普遍存在。")
        resp_list.append("2、请核实库存车辆的所有权属，建议收集车辆行驶证等资产证明材料。")
        resp_list.append("3、请关注实时行业政策，该行业受国家消费、能耗、技术等方面政策影响较大。例如：取消新能源汽车的补贴。")
    elif industry_code in ['F5123']:
        resp_list.append("1、请关注水果市场变化，该行业受需求供给状况、行业政策、气候变化的影响较大。")
        resp_list.append("2、请关注客户的存货周转率是否过低，注意与同行业同类型产品比较。")
        resp_list.append("3、如果客户进货渠道单一，请谨慎授信。")
        resp_list.append("4、该行业淡旺季明显，建议将贷款到期日设置在销售旺季。")
    elif industry_code in ['E4813']:
        resp_list.append("1、请关注客户的资质完备情况，没有建筑施工资质的客户往往采用挂靠的形式开展业务，可以核实客户的挂靠协议或项目合同。")
        resp_list.append("2、请关注法律风险，该行业易产生交通事故、买卖合同纠纷、劳务纠纷、借款纠纷。")
        resp_list.append("3、请关注隐形负债，该行业内垫资、拆借及民间借贷情况普遍存在。")
        resp_list.append("4、该行业客户固定资产持有率高，如果借款人或实际控制人固定资产少、保障性弱，请谨慎授信。")
        resp_list.append("5、该行业客户资金调动能力较强，如果客户现金流差、资金调动能力弱，请谨慎授信。")
    elif industry_code in ['P82']:
        resp_list.append("1、请关注客户的资质完备情况，教育行业办学资质证件含《办学许可证》、《营业执照》等。")
        resp_list.append("2、请关注公司扩张速度是否与经营规模匹配，如果扩张速度过快，可能导致现金流断裂。")
        resp_list.append("3、请关注该行业的声誉以及信誉风险，可通过员工素质、知识水平以及管理规范等方面了解。")
        resp_list.append("4、请关注客户的机构品牌的市场认可度，如品牌为自创品牌或市场受众较小，请谨慎授信。")
    elif industry_code in ['F516']:
        resp_list.append("1、请关注客户经营产品品牌的市场认可度，如产品品牌较为小众，请谨慎授信。")
        resp_list.append("2、请关注产品市场价格波动风险。")
        resp_list.append("3、请核实应收账款质量。一般国企、政府的应收回款保障较高，但是账期会较长；外企的应收质量较高，且回款稳定；私企的应收稳定性较弱，坏账风险更大。")
        resp_list.append("4、该行业客户资产存货和应收占比高，经营风险相对较大，如果客户固定资产少，保障性弱，请谨慎授信。")
        resp_list.append("5、该行业的日均与资金调动能力一般高于其他行业，如果客户资金调动能力较差，请谨慎授信。")
    elif industry_code in ['H61']:
        resp_list.append("1、请关注客户的资质完备情况，酒店行业基本资质证件含《营业执照》、《消防证》、《特种行业许可证》、《卫生许可证齐全》，且证件地址需与酒店地址一致。")
        resp_list.append("2、请关注酒店物业的剩余租期是否在贷款期限内，警惕到期不能续租的情况。")
        resp_list.append("3、请关注酒店经营的年限，以及经营者酒店行业的从业年限，如果酒店经营1年以内，或经营者酒店行业从业2年以内的，请谨慎授信。")
        resp_list.append("4、请关注酒店投资时间以及金额，酒店价值随投资年限增长而下降。")
        resp_list.append("5、该行业一般会有多人合伙投资占股情况，请核实酒店的实际控制人、真实股份构成、分红方式以及企业主其他多元化对外投资情况。")
        resp_list.append("6、如果客户经营非连锁品牌的低端宾馆或一般旅馆，请谨慎授信。")
        resp_list.append("7、如果客户不参与名下所有占股酒店的实际经营，请谨慎授信。")
    elif industry_code in ['F5124']:
        resp_list.append("1、请关注客户的资质完备情况，肉类批发行业基本资质证件含《营业执照》、《食品流通许可证齐全》。")
        resp_list.append("2、请关注产品市场价格波动风险。")
        resp_list.append("3、请关注应收账款质量和账期长短，该行业普遍存在应收账款坏账风险和账期不稳定情况。")
        resp_list.append("4、请关注客户的存货周转率与同行业同类型产品相比是否过低。")
        resp_list.append(
            "5、该行业淡旺季明显，建议将贷款到期日设置在销售旺季。牛羊肉、鸡鸭肉冻品旺季一般为10-3月；水产虾蟹生鲜的销售旺季一般为4-7月、9-12月，以及端午、中秋、国庆等节假日；海鲜冻品的销售旺季一般在休渔期。")
    elif industry_code in ['F5274']:
        resp_list.append("1、请关注企业主手机门店是否有品牌授权以及是否在授权期限内。")
        resp_list.append("2、请关注客户销售手机的品牌，国内目前华为、vivo、OPPO、小米、苹果5个品牌的市场占有率达90%，如果客户主营产品非主流品牌，建议谨慎授信。")
        resp_list.append("3、请关注是否有存货积压的风险。")
        resp_list.append("4、请关注公司扩张速度是否与经营规模匹配，如果扩张速度过快，可能导致现金流断裂，甚至过度举债经营。")
        resp_list.append("5、请关注门店地段的人口密集程度。")
    elif industry_code in ['F512', 'F5127']:
        resp_list.append("1、请关注厂商压账的风险，以及代理商每年销售指标是否能够完成。")
        resp_list.append("2、请关注隐形负债，该行业厂家授信或通过其他金融机构授信的情况普遍存在。")
        resp_list.append("3、请关注客户的存货周转率与同行业同类型产品是否过低。")
        resp_list.append("4、如果客户经营产品为饮料、啤酒等，产品保质期要求较严格的，请关注固定资产保障。")
        resp_list.append("5、请关注经营产品的品牌是否有市场竞争力、销售渠道是否稳定。")
        resp_list.append("6、该行业淡旺季明显，建议将贷款到期日设置在销售旺季")
    elif industry_code in ['F5283']:
        resp_list.append("1、请关注库存积压情况。")
        resp_list.append("2、请关注线下零售门店所在专业市场的成熟度及客流量。")
        resp_list.append("3、请关注零售品牌的市场认可度和销售渠道。")
        resp_list.append("4、请关注仓库的防火措施是否到位。")
        resp_list.append("5、该行业属于夕阳行业，请谨慎授信。")
    elif industry_code in ['F5137']:
        resp_list.append("1、请关注隐形负债，该行业厂家授信或通过其他金融机构授信的情况普遍存在。")
        resp_list.append("2、请关注经营产品的品牌是否有市场竞争力、销售渠道是否稳定。")
        resp_list.append("3、请关注线下零售门店所在专业市场的客流量。")
        resp_list.append("4、请关注厂商压账的风险，以及代理商每年销售指标是否能够完成。")
        resp_list.append("5、请关注库存积压情况。")
        resp_list.append("6、如果客户经营的产品种类多，销量低，请谨慎授信。")
    elif industry_code in ['F513', 'F5132']:
        resp_list.append("1、请关注应收账款质量和账期长短，该行业普遍存在应收账款金额较大、账期长、坏账率高的情况。")
        resp_list.append("2、请关注该行业客户是否有选款不慎，压货过多的情况。")
        resp_list.append("3、请关注隐形负债，该行业库存、应收资金占用较大，厂房、设备投入较多，人工工资等运营成本较高，设备融资租赁、民间借贷等情况普遍存在。")
        resp_list.append("4、请关注资产保障性和体外担保设置的合理性，建议有增信措施。")
    return resp_list