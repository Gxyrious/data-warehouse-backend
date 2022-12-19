from flask import request, Blueprint, jsonify
from mysql_.model import *

byactor = Blueprint("mysql_byactor", __name__)

@byactor.route('/movie', methods=['GET'])
def getMovieNumByActor():
    data = request.args
    actor = data.get('actor')
    movie_num = db.session.query(Act.movie_id) \
        .join(Actor, Actor.actor_id == Act.actor_id) \
        .filter(Actor.name.like("%{}%".format(actor))) \
        .count()

    return jsonify({"movie_num": movie_num})