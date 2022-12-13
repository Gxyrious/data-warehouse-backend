
from flask import Flask
from flask_cors import CORS

# 引入mysql
from mysql_.model import *
from mysql_.api.bytime import bytime as mysql_bytime
from mysql_.api.bytitle import bytitle as mysql_bytitle
from mysql_.api.bydirector import bydirector as mysql_bydirector
from mysql_.api.byactor import byactor as mysql_byactor
from mysql_.api.cooperation import cooperation as mysql_cooperation
from mysql_.api.bygenre import bygenre as mysql_bygenre
from mysql_.api.byreview import byreview as mysql_byreview
from mysql_.api.comprehensive import comprehensive as mysql_comprehensive

# 引入neo4j
from neo4j_.api.bytime import bytime as neo4j_bytime
from neo4j_.api.bytitle import bytitle as neo4j_bytitle

app = Flask(__name__)
CORS(app, resources=r'/*')	# 注册CORS, "/*" 允许访问所有api

# 注册mysql相关api
app.register_blueprint(mysql_bytime, url_prefix='/mysql/bytime')
app.register_blueprint(mysql_bytitle, url_prefix='/mysql/bytitle')
app.register_blueprint(mysql_bydirector, url_prefix='/mysql/bydirector')
app.register_blueprint(mysql_byactor, url_prefix='/mysql/byactor')
app.register_blueprint(mysql_cooperation, url_prefix='/mysql/cooperation')
app.register_blueprint(mysql_bygenre, url_prefix='/mysql/bygenre')
app.register_blueprint(mysql_byreview, url_prefix='/mysql/byreview')
app.register_blueprint(mysql_comprehensive, url_prefix='/mysql/comprehensive')

# 注册neo4j相关api
app.register_blueprint(neo4j_bytime, url_prefix='/neo4j/bytime')
app.register_blueprint(neo4j_bytitle, url_prefix='/neo4j/bytitle')

class Config():

    SQLALCHEMY_DATABASE_URI = "mysql://liuchang:tjdxDW2022@81.68.102.171:3306/dw" # 后续ip地址改为localhost
    # SQLALCHEMY_TRACK_MODIFICATIONS = False
    # SQLALCHEMY_ECHO = True

app.config.from_object(Config)

db.init_app(app)

if __name__ == '__main__':
    with app.app_context():
        db.create_all()
        app.run(host='0.0.0.0', debug=True)


### 后台运行
# nohup python main.py &
### 寻找pid杀死
# ps -aux|grep -v grep |grep main.py |awk '{print $2}'| xargs kill -9