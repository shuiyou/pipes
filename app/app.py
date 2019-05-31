import socket

from flask import Flask

app = Flask(__name__)


@app.route("/", methods=['GET', 'POST'])
def dispatch():
    '''
    应用的统一入口，获取数据分发给不通的数据映射和决策，然后返回结果
    :return:
    '''
    return "Hello from FLASK. My Hostname is: %s \n" % (socket.gethostname())


if __name__ == '__main__':
    app.run(host='0.0.0.0')
