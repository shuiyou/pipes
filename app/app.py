import os

from flask import Flask, request, jsonify

app = Flask(__name__)
# 配置使用的决策引擎
app.config['STRATEGY_URL'] = os.getenv('STRATEGY_URL', 'http://192.168.1.20:8091')


@app.route("/", methods=['POST'])
def dispatch():
    '''
    应用的统一入口，获取数据分发给不通的数据映射和决策，然后返回结果
    :return:
    '''
    # 获取请求参数
    json_data = request.get_json()
    # TODO: 实现dispatcher, 根据json_data的指令去获取数据，做对应的数据处理，然后调用对应的决策

    return jsonify(json_data)


if __name__ == '__main__':
    app.run(host='0.0.0.0')
