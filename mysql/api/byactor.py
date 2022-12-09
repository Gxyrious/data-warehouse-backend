from flask import request, Blueprint
from mysql.model import *

byactor = Blueprint("byactor", __name__)

@byactor.route('/movie', methods=['GET'])
def getMovieNumByActor():
    data = request.args
    actor = data.get('actor')
    movie_num = db.session.query(Act.movie_id) \
        .join(Actor, Actor.actor_id == Act.actor_id) \
        .filter(Actor.name.like("%{}%".format(actor))) \
        .count()

    return str({"movie_num": movie_num})