from flask import request, Blueprint, jsonify
from neo4j_.model import graph
from py2neo import NodeMatcher

bytitle = Blueprint("neo4j_bytitle", __name__)

# http://81.68.102.171:5000/neo4j/bytitle/change-your-route-here
@bytitle.route('/change-your-route-here')
def useStyleLikeThis():
    nodematcher = NodeMatcher(graph)
    movies = nodematcher.match("movie").count()
    return jsonify(movies)