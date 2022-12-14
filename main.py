
from flask import Flask
from flask_cors import CORS

# import spark
from spark_.api.comprehensive import comprehensive as spark_comprehensive
from spark_.api.cooperation import cooperation as spark_cooperation
from spark_.utils import get_spark_session

app = Flask(__name__)
CORS(app, resources=r'/*')  # 注册CORS, "/*" 允许访问所有api

# register spark
app.register_blueprint(spark_cooperation, url_prefix='/spark/cooperation')
app.register_blueprint(spark_comprehensive, url_prefix='/spark/comprehensive')

if __name__ == '__main__':
    get_spark_session()
    with app.app_context():
        app.run(host='0.0.0.0', debug=True)


# 后台运行
# nohup python main.py &
# 寻找pid杀死
# ps -aux|grep -v grep |grep main.py |awk '{print $2}'| xargs kill -9
