from logger.logstash import LogstashFormatter
from views import app

LogstashFormatter

if __name__ == '__main__':
    app.run(host='0.0.0.0')