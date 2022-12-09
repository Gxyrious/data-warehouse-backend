from flask import request, Blueprint
from mysql.model import *

bytime = Blueprint("bytime", __name__)

@bytime.route('/year', methods=['GET'])
def getNumByYear():
    data = request.args
    year = data.get('year')
    movie_num = db.session.query(ReleaseDate.movie_id) \
        .filter(ReleaseDate.year == year) \
        .count()

    return str({"movie_num": movie_num})


@bytime.route('/year-month', methods=['GET'])
def getMovieNumByYearAndMonth():
    data = request.args
    year, month = data.get('year'), data.get('month')
    movie_num = db.session.query(ReleaseDate.movie_id) \
        .filter(ReleaseDate.year == year, ReleaseDate.month == month) \
        .count()
    return str({"movie_num": movie_num})

@bytime.route('/year-season', methods=['GET'])
def getMovieNumByYearAndSeason():
    data = request.args
    year, season = data.get('year'), data.get('season')
    movie_num = db.session.query(ReleaseDate.movie_id) \
        .filter(ReleaseDate.year == year, ReleaseDate.season == season) \
        .count()
    return str({"movie_num": movie_num})

@bytime.route('/weekday', methods=['GET'])
def getNumByWeek():
    data = request.args
    weekday = data.get('weekday')
    movie_num = db.session.query(ReleaseDate.weekday) \
        .filter(ReleaseDate.weekday == weekday) \
        .count()
    return str({"movie_num": movie_num})