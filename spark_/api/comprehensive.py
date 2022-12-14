# comprehensive search
from flask import request, Blueprint, jsonify
import time
import json
from spark_.utils import get_spark_session

comprehensive = Blueprint("spark_comprehensive", __name__)


@comprehensive.route('/movie', methods=['POST'])
def spark_comprehensive_movie():
    # get data from post
    conditions = request.get_json()
    print(conditions)

    columns = conditions['columns']

    session = get_spark_session()
    movies = session.sql('select * from movie')

    result = {'data': []}

    # start time
    start_time = time.time()

    # select from database
    df = movies.filter(movies.movie_id < 20).select(columns)

    # end time
    end_time = time.time()
    
    result['data'] = df.rdd.map(lambda row: row.asDict()).collect()

    consuming_time = end_time - start_time
    result.update({'consuming_time': consuming_time})
    return jsonify(result)
