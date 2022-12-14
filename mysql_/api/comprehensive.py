from flask import request, Blueprint, jsonify
from mysql_.model import *
import time, sqlalchemy
from collections import Counter

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


@comprehensive.route('/relation', methods=['POST'])
def comprehensiveRelationQuery():
    consuming_time = 0
    data: dict = request.get_json()
    name, times = data["name"], int(data["times"])
    source, target = data["source"], data["target"]

    if source == "director" and target == "actor":
        consuming_time, result = __getActorCooperateWithDirector(name, times)
    elif source == "actor" and target == "actor":
        consuming_time, result = __getActorCooperateWithActor(name, times)
    elif source == "actor" and target == "director":
        consuming_time, result = __getDirectorCooperateWithActor(name, times)
    else:
        print(source, target)
    
    return jsonify({
        "count": len(result),
        "consuming_time": consuming_time,
        "data": result
    })

def __getActorCooperateWithDirector(director, times):

    # 查找与director合作次数超过time的actor
    director_cooperate_actor = db.session.query(
            Director.name.label("director_name"), 
            t_Cooperation.c.left_person_id.label("director_id"), 
            # Director.director_id.label("director_id"),
            Actor.name.label("actor_name"), 
            t_Cooperation.c.right_person_id.label("actor_id"), 
            # Actor.actor_id.label("actor_id"), 
            t_Cooperation.c.movie_id.label("movie_id")
        ) \
        .filter(
            t_Cooperation.c.type == 1, 
            Director.director_id == t_Cooperation.c.left_person_id, 
            Actor.actor_id == t_Cooperation.c.right_person_id
        ) \
        .subquery()

    # 子查询，subquery.c表示从自定的Column列中找
    actors_query = db.session.query(director_cooperate_actor.c.actor_name) \
        .filter(director_cooperate_actor.c.director_name.like("%{}%".format(director))) \
        .group_by(
            director_cooperate_actor.c.director_id,
            director_cooperate_actor.c.actor_id
        ) \
        .having(sqlalchemy.func.count(director_cooperate_actor.c.movie_id) >= times)
    
    start_time = time.time()
    actors = actors_query.all()
    consuming_time = time.time() - start_time

    return consuming_time, list(map(lambda x: x[0], actors))

def __getActorCooperateWithActor(actor, times):

    # 查找与actor合作次数超过time的actor
    left_actors: Actor = sqlalchemy.orm.aliased(Actor)
    right_actors: Actor = sqlalchemy.orm.aliased(Actor)

    actor_cooperate_actor = db.session.query(
            left_actors.name.label("left_actor_name"),
            t_Cooperation.c.left_person_id.label("left_actor_id"),
            # left_actors.actor_id.label("left_actor_id"),
            right_actors.name.label("right_actor_name"),
            t_Cooperation.c.right_person_id.label("right_actor_id"),
            # right_actors.actor_id.label("right_actor_id"),
            t_Cooperation.c.movie_id.label("movie_id")
        ) \
        .filter(
            t_Cooperation.c.type == 2,
            left_actors.actor_id == t_Cooperation.c.left_person_id,
            right_actors.actor_id == t_Cooperation.c.right_person_id
        ) \
        .subquery()

    left_query = db.session.query(
            # actor_cooperate_actor.c.left_actor_id.label("with_actor_id"),
            actor_cooperate_actor.c.left_actor_name.label("with_actor_name")
        ) \
        .filter(actor_cooperate_actor.c.right_actor_name.like("%{}%".format(actor)))
    right_query = db.session.query(
            # actor_cooperate_actor.c.right_actor_id.label("with_actor_id"),
            actor_cooperate_actor.c.right_actor_name.label("with_actor_name")
        ) \
        .filter(actor_cooperate_actor.c.left_actor_name.like("%{}%".format(actor)))

    final_query = left_query.union_all(right_query)

    start_time = time.time()
    actors_all = final_query.all()
    consuming_time = time.time() - start_time

    # actors_all = final_query.all() # 查询
    # delete_bracket = map(lambda x: x[0], actors_all) # 删除括号，取第一个
    # count_occurrence_time = dict(Counter(delete_bracket)) # 计算出现次数，转换为字典
    # time_filted = filter(lambda x: x[1] >= time, count_occurrence_time.items()) # 过滤
    # get_actor_name = list(map(lambda x: x[0], time_filted)) # 获取姓名
    # return str(get_actor_name)

    return consuming_time, list(map(lambda x: x[0], filter(lambda x: x[1] >= times, dict(Counter(map(lambda x: x[0], actors_all))).items())))

def __getDirectorCooperateWithActor(actor, times):

    # 查找与actor合作次数超过time的director
    director_cooperate_actor = db.session.query(
            Director.name.label("director_name"),
            t_Cooperation.c.left_person_id.label("director_id"),
            # Director.director_id.label("director_id"),
            Actor.name.label("actor_name"), 
            t_Cooperation.c.right_person_id.label("actor_id"),
            # Actor.actor_id.label("actor_id"),
            t_Cooperation.c.movie_id.label("movie_id"),
        ) \
        .filter(
            t_Cooperation.c.type == 1,
            Director.director_id == t_Cooperation.c.left_person_id,
            Actor.actor_id == t_Cooperation.c.right_person_id
        ) \
        .subquery()

    directors_query = db.session.query(director_cooperate_actor.c.director_name) \
        .filter(director_cooperate_actor.c.actor_name.like("%{}%".format(actor))) \
        .group_by(
            director_cooperate_actor.c.director_id,
            director_cooperate_actor.c.actor_id
        ) \
        .having(sqlalchemy.func.count(director_cooperate_actor.c.movie_id) >= times)

    start_time = time.time()
    directors = directors_query.all()
    consuming_time = time.time() - start_time

    return consuming_time, list(map(lambda x: x[0], directors))
