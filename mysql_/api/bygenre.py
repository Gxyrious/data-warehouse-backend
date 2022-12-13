from flask import request, Blueprint, jsonify
from mysql_.model import *

bygenre = Blueprint("mysql_bygenre", __name__)

@bygenre.route('/movie', methods=['GET'])
def getMovieNumByGenre():
    data = request.args
    genre = data.get('genre')
    movie_num = db.session.query(Genre.movie_id) \
        .filter(Genre.genre_name.like("%{}%".format(genre))) \
        .count()

    return jsonify({"movie_num": movie_num})