from flask import request, Blueprint, jsonify

bytime = Blueprint("neo4j_bytime", __name__)

# http://81.68.102.171:5000/neo4j/bytime/change-your-route-here
@bytime.route('/change-your-route-here')
def useStyleLikeThis():
    data = {"example": 123456}
    return jsonify(data)