from flask import request, Blueprint, jsonify
from mysql_.model import *
import time, sqlalchemy

comprehensive = Blueprint("mysql_comprehensive", __name__)

@comprehensive.route('/movie', methods=['POST'])
def comprehensiveMovieQuery():
    consuming_time = 0
    data: dict = request.get_json()
    print(data)
    columns = data.pop("columns")
    # page, per_page = data.pop("page"), data.pop("per_page")
    
    # 获取简单的字段，直接查视图
    # ["score", "edition", "date", "genre_name", "title", "actors", "directors", "format", "asin", ]
    simple_columns = list({"score", "edition", "date", "genre_name", "title"}.intersection(set(columns)))
    simple_columns.append("movie_id")
    queries = [t_movie_date_genre.c[query] for query in simple_columns]
    # 获取查询条件
    filters = []
    for key, value in data.items():
        if key == "genre_name":
            filters.append(t_movie_date_genre.c["genre_name"].like("%{}%".format(value)))
        elif key == "title":
            filters.append(t_movie_date_genre.c["title"].like("%{}%".format(value)))
        elif key == "format":
            filters.append(t_movie_format.c["format_name"].like("%{}%".format(value)))
            filters.append(t_movie_format.c["movie_id"] == t_movie_date_genre.c["movie_id"])
        elif key == "actor":
            filters.append(t_movie_actor.c["name"].like("%{}%".format(value)))
            filters.append(t_movie_actor.c["movie_id"] == t_movie_date_genre.c["movie_id"])
        elif key == "director":
            filters.append(t_movie_director.c["name"].like("%{}%".format(value)))
            filters.append(t_movie_director.c["movie_id"] == t_movie_date_genre.c["movie_id"])
        elif key == "min_score":
            filters.append(t_movie_date_genre.c["score"] >= value)
        elif key == "max_score":
            filters.append(t_movie_date_genre.c["score"] <= value)
        elif key in ("year", "month", "day", "season", "weekday"):
            filters.append(t_movie_date_genre.c[key] == value)
        else:
            print(key, value)
    start_time = time.time()
    result_of_simple_columns = db.session.query(*queries).filter(*filters).all()
    consuming_time += time.time() - start_time
    result = []
    for row in result_of_simple_columns:
        single_result = {}
        for index in range(len(queries)):
            if simple_columns[index] == "movie_id":
                continue
            elif simple_columns[index] == "date":
                datetime: sqlalchemy.DateTime = row[index]
                single_result[simple_columns[index]] = str(datetime).split(' ')[0]
            else:
                single_result[simple_columns[index]] = row[index]
        movie_id = row[-1]
        # print("movie_id = ", movie_id)
        if "actors" in columns:
            start_time = time.time()
            actors = db.session.query(t_movie_actor.c["name"]) \
                .filter(t_movie_actor.c["movie_id"] == movie_id) \
                .all()
            consuming_time += time.time() - start_time
            # print("actors = ", actors)
            single_result["actors"] = list(map(lambda x: x[0], actors))
        if "directors" in columns:
            start_time = time.time()
            directors = db.session.query(t_movie_director.c["name"]) \
                .filter(t_movie_director.c["movie_id"] == movie_id) \
                .all()
            consuming_time += time.time() - start_time
            # print("directors = ", directors)
            single_result["directors"] = list(map(lambda x: x[0], directors))
        if "format" in columns:
            start_time = time.time()
            formats = db.session.query(t_movie_format.c["format_name"]) \
                .filter(t_movie_format.c["movie_id"] == movie_id) \
                .all()
            consuming_time += time.time() - start_time
            # print("formats = ", formats)
            single_result["formats"] = list(map(lambda x: x[0], formats))
        if "asin" in columns:
            start_time = time.time()
            asins = db.session.query(Asin.asin) \
                .filter(Asin.movie_id == movie_id) \
                .all()
            consuming_time += time.time() - start_time
            single_result["asin"] = list(map(lambda x: x[0], asins))
        result.append(single_result)
    # print(result)
    return jsonify({
            "count": len(result),
            "consuming_time": consuming_time,
            "data": result,
        })
