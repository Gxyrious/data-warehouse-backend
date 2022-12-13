from flask import request, Blueprint, jsonify
from mysql_.model import *

bydirector = Blueprint("mysql_bydirector", __name__)

@bydirector.route('/movie', methods=['GET'])
def getMovieNumByDirector():
    data = request.args
    director = data.get('director')
    movie_num = db.session.query(Direct.movie_id) \
        .join(Director, Director.director_id == Direct.director_id) \
        .filter(Director.name.like("%{}%".format(director))) \
        .count()

    return jsonify({"movie_num": movie_num})