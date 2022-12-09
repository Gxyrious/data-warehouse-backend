
from flask import Flask, request

from mysql.model import *
from mysql.api.bytime import bytime
from mysql.api.bytitle import bytitle
from mysql.api.bydirector import bydirector
from mysql.api.byactor import byactor
from mysql.api.cooperation import cooperation
from mysql.api.bygenre import bygenre
from mysql.api.byreview import byreview

app = Flask(__name__)

app.register_blueprint(bytime, url_prefix='/api/bytime')
app.register_blueprint(bytitle, url_prefix='/api/bytitle')
app.register_blueprint(bydirector, url_prefix='/api/bydirector')
app.register_blueprint(byactor, url_prefix='/api/byactor')
app.register_blueprint(cooperation, url_prefix='/api/cooperation')
app.register_blueprint(bygenre, url_prefix='/api/bygenre')
app.register_blueprint(byreview, url_prefix='/api/byreview')

class Config():

    SQLALCHEMY_DATABASE_URI = "mysql://root:FBCFBCFBC@43.142.164.87:3306/dw_movie"
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
# ps aux
# kill -9 PID