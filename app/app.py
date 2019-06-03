import os

import requests
from flask import Flask, request, jsonify

app = Flask(__name__)
# 配置使用的决策引擎
app.config['STRATEGY_URL'] = os.getenv('STRATEGY_URL', 'http://192.168.1.20:8091/rest/S1Public')


def _build_request(input):
    """
    根据http请求构建出决策需要的请求, 需要去查数据库获取相关的数据
    :return:
    """
    # TODO: 实现请求，数据映射的代码请写在mapping包里
    return {"StrategyOneRequest": {
        "Header": {"InquiryCode": "c3ef30f0ad5646d8a25136f98532ec9f", "ProcessCode": "JB_WZ_CJR2"},
        "Body": {"Application": {"Variables": {}}}}}


def build_response(json):
    return json


@app.route("/", methods=['POST'])
def dispatch():
    '''
    应用的统一入口，获取数据分发给不通的数据映射和决策，然后返回结果
    :return:
    '''
    # 获取请求参数
    json_data = request.get_json()
    # TODO: 实现dispatcher, 根据json_data的指令去获取数据，做对应的数据处理，然后调用对应的决策
    strategy_request = _build_request(json_data)
    # 调用决策引擎
    strategy_response = requests.post(app.config['STRATEGY_URL'], json=strategy_request)
    # TODO：需要转换成约定好的输出schema形势
    return jsonify(build_response(strategy_response.json()))


@app.route("/health", methods=['GET'])
def health_check():
    return 'pipes is up'


if __name__ == '__main__':
    app.run(host='0.0.0.0')
