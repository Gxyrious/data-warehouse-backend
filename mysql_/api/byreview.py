from flask import request, Blueprint, jsonify
from sqlalchemy import func
from mysql_.model import *

byreview = Blueprint("mysql_byreview", __name__)

@byreview.route('/score', methods=['GET'])
def getMovieTitleByReviewScore():
    data = request.args
    score = int(data.get('score'))
    page, size = int(data.get('page')), int(data.get('size'))
    titles = db.session.query(Movie.title) \
        .join(Review, Review.movie_id == Movie.movie_id) \
        .group_by(Review.movie_id) \
        .having(func.avg(Review.review_score) >= score) \
        .paginate(page=page, per_page=size).items
    
    return jsonify(titles)