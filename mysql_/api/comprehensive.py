from flask import request, Blueprint
from mysql_.model import *
import json

comprehensive = Blueprint("mysql_comprehensive", __name__)

@comprehensive.route('/movie', methods=['GET'])
def comprehensiveQuery():
    print("进入函数")
    data = request.args
    json_data = json.loads(data.get('json'))
    print(json_data)

    title = json_data.get('title')
    # if title:

    # print(title)
    date = json_data.get('date')
    print(date)
    score = json_data.get('score')
    print(score)
    actor = json_data.get('actor')
    print(actor)
    director = json_data.get('director')
    print(director)
    genre = json_data.get('genre')
    print(genre)
    columns = json_data.get('columns')
    print(columns)
    return "ok"




