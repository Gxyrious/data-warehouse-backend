from flask import request, Blueprint, jsonify

bytitle = Blueprint("neo4j_bytitle", __name__)

# http://81.68.102.171:5000/neo4j/bytitle/change-your-route-here
@bytitle.route('/change-your-route-here')
def useStyleLikeThis():
    data = {"example": 123456}
    return jsonify(data)