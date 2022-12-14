# cooperation search
from flask import request, Blueprint, jsonify
import time

cooperation = Blueprint("spark_cooperation", __name__)

@cooperation.route('/relation', methods=['POST'])
def spark_relation():
    # get data from post
    conditions = request.get_json()
    print(conditions)
    result = {}
    # calculate time
    start_time = time.time()

    # select from database
    

    end_time = time.time()
    consuming_time = end_time - start_time
    result.update({'consuming_time': consuming_time})
    return jsonify(result)
