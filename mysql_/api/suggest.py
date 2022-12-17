from flask import request, Blueprint, jsonify
from mysql_.model import *
from sqlalchemy import func

suggest = Blueprint("suggest", __name__)

@suggest.route("/movie", methods=['GET'])
def getMovieSuggestion():
    data = request.args
    title, amount = data["title"], data["amount"]
    suggestions = db.session.query(Movie.title) \
        .filter(Movie.title.like("{}%".format(title))) \
        .limit(amount) \
        .all()
    result = list(map(lambda x: x[0], suggestions))
    return jsonify({"suggestions": result})

@suggest.route("/actor", methods=['GET'])
def getActorSuggestion():
    data = request.args
    actor, amount = data["actor"], data["amount"]
    suggestions = db.session.query(Actor.name) \
        .filter(Actor.name.like("{}%".format(actor))) \
        .limit(amount) \
        .all()
    result = list(map(lambda x: x[0], suggestions))
    return jsonify({"suggestions": result})

@suggest.route("/director", methods=['GET'])
def getDirectorSuggestion():
    data = request.args
    director, amount = data["director"], data["amount"]
    suggestions = db.session.query(Director.name) \
        .filter(Director.name.like("{}%".format(director))) \
        .limit(amount) \
        .all()
    result = list(map(lambda x: x[0], suggestions))
    return jsonify({"suggestions": result})

@suggest.route("/genre", methods=['GET'])
def getGenreSuggestion():
    data = request.args
    genre, amount = data["genre"], data["amount"]
    suggestions = db.session.query(func.distinct(Genre.genre_name)) \
        .filter(Genre.genre_name.like("{}%".format(genre))) \
        .limit(amount) \
        .all()
    result = list(map(lambda x: x[0], suggestions))
    return jsonify({"suggestions": result})