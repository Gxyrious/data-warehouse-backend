from flask import request, Blueprint, jsonify
from sqlalchemy import distinct
from mysql_.model import *

bytitle = Blueprint("mysql_bytitle", __name__)

@bytitle.route('/format', methods=['GET'])
def getFormatNumByTitle():
    data = request.args
    title = data.get('title')
    format_num = db.session.query(distinct(Format.format_name)) \
        .filter(Format.movie_title.like("%{}%".format(title))) \
        .count()
    
    return jsonify({"format_num": format_num})