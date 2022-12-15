from flask import request, Blueprint, jsonify
from mysql_.model import *
import time, sqlalchemy
from collections import Counter

comprehensive = Blueprint("mysql_comprehensive", __name__)

@comprehensive.route('/movie', methods=['POST'])
def comprehensiveMovieQuery():
    consuming_time = 0
    data: dict = request.get_json()
    columns = data.pop("columns")
    page, per_page = 1, 10
    try:
        page, per_page = int(data.pop("page")), int(data.pop("per_page"))
    except KeyError as e:
        print(e)

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
            raise KeyError("筛选条件字段错误，只能包含genre_name, title, format, actor, director, min_score, max_score, year, month, day, season, weekday以及目标字典columns列表")
    start_time = time.time()
    result_of_simple_columns = db.session.query(*queries).filter(*filters).paginate(page=page, per_page=per_page).items
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
    page, per_page = 1, 10
    try:
        page, per_page = int(data.pop("page")), int(data.pop("per_page"))
    except KeyError as e:
        print(e)
    if source == "director" and target == "actor":
        consuming_time, result = __getActorCooperateWithDirector(name, times, page, per_page)
    elif source == "actor" and target == "actor":
        consuming_time, result = __getActorCooperateWithActor(name, times, page, per_page)
    elif source == "actor" and target == "director":
        consuming_time, result = __getDirectorCooperateWithActor(name, times, page, per_page)
    else:
        print(source, target)
    
    return jsonify({
        "count": len(result),
        "consuming_time": consuming_time,
        "data": result
    })

def __getActorCooperateWithDirector(director, times, page, per_page):

    # 查找与director合作次数超过time的actor
    director_cooperate_actor = db.session.query(
            Director.name.label("director_name"), 
            t_Cooperation.c.left_person_id.label("director_id"), 
            # Director.director_id.label("director_id"),
            Actor.name.label("actor_name"), 
            t_Cooperation.c.right_person_id.label("actor_id"), 
            # Actor.actor_id.label("actor_id"), 
            t_Cooperation.c.movie_id.label("movie_id"),
            Movie.title.label("title")
        ) \
        .filter(
            t_Cooperation.c.type == 1, 
            Director.director_id == t_Cooperation.c.left_person_id, 
            Actor.actor_id == t_Cooperation.c.right_person_id,
            Movie.movie_id == t_Cooperation.c.movie_id
        ) \
        .subquery()

    # 子查询，subquery.c表示从自定的Column列中找
    actors_query = db.session.query(
            director_cooperate_actor.c.actor_name,
            sqlalchemy.func.group_concat(director_cooperate_actor.c.title),
            sqlalchemy.func.count(director_cooperate_actor.c.movie_id),
        ) \
        .filter(director_cooperate_actor.c.director_name.like("%{}%".format(director))) \
        .group_by(
            director_cooperate_actor.c.director_id,
            director_cooperate_actor.c.actor_id
        ) \
        .having(sqlalchemy.func.count(director_cooperate_actor.c.movie_id) >= times)
    
    start_time = time.time()
    actors = actors_query.paginate(page=page, per_page=per_page).items
    consuming_time = time.time() - start_time

    return consuming_time, list(map(lambda x: {"name": x[0], "movies": x[1].split(','), "times": x[2]}, actors))

def __getActorCooperateWithActor(actor, times, page, per_page):

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
            t_Cooperation.c.movie_id.label("movie_id"),
            Movie.title.label("title")
        ) \
        .filter(
            t_Cooperation.c.type == 2,
            left_actors.actor_id == t_Cooperation.c.left_person_id,
            right_actors.actor_id == t_Cooperation.c.right_person_id,
            Movie.movie_id == t_Cooperation.c.movie_id
        ) \
        .subquery()

    left_query = db.session.query(
            # actor_cooperate_actor.c.left_actor_id.label("with_actor_id"),
            actor_cooperate_actor.c.left_actor_name.label("with_actor_name"),
            
            actor_cooperate_actor.c.title.label("title"),
        ) \
        .filter(actor_cooperate_actor.c.right_actor_name.like("%{}%".format(actor)))
    right_query = db.session.query(
            # actor_cooperate_actor.c.right_actor_id.label("with_actor_id"),
            actor_cooperate_actor.c.right_actor_name.label("with_actor_name"),
            # actor_cooperate_actor.c.title.label("title"),
            actor_cooperate_actor.c.title.label("title"),
        ) \
        .filter(actor_cooperate_actor.c.left_actor_name.like("%{}%".format(actor)))
    final_query = left_query.union_all(right_query)

    start_time = time.time()
    actors_all = final_query.paginate(page=page, per_page=per_page).items
    consuming_time = time.time() - start_time

    # actors_all = final_query.all() # 查询
    delete_bracket = list(map(lambda x: (x[0],x[1]), actors_all)) # 删除括号，取第一个
    result_dict = {}
    for bracket in delete_bracket:
        name, movie = bracket[0], bracket[1]
        if name in result_dict.keys():
            result_dict[name].append(movie)
        else:
            result_dict[name] = [movie]
    result = []
    for key, value in result_dict.items():
        if len(value) >= times:
            result.append({
                "name": key,
                "movies": value,
                "times": len(value)
            })
    return consuming_time, result

    # return consuming_time, list(map(lambda x: [x[0], x[1]], filter(lambda x: x[1] >= times, dict(Counter(map(lambda x: [x[0], x[1]], actors_all))).items())))

def __getDirectorCooperateWithActor(actor, times, page, per_page):

    # 查找与actor合作次数超过time的director
    director_cooperate_actor = db.session.query(
            Director.name.label("director_name"),
            t_Cooperation.c.left_person_id.label("director_id"),
            # Director.director_id.label("director_id"),
            Actor.name.label("actor_name"), 
            t_Cooperation.c.right_person_id.label("actor_id"),
            # Actor.actor_id.label("actor_id"),
            t_Cooperation.c.movie_id.label("movie_id"),
            Movie.title.label("title")
        ) \
        .filter(
            t_Cooperation.c.type == 1,
            Director.director_id == t_Cooperation.c.left_person_id,
            Actor.actor_id == t_Cooperation.c.right_person_id,
            Movie.movie_id == t_Cooperation.c.movie_id
        ) \
        .subquery()

    directors_query = db.session.query(
            director_cooperate_actor.c.director_name,
            sqlalchemy.func.group_concat(director_cooperate_actor.c.title),
            # sqlalchemy.func.group_concat(sqlalchemy.func.concat_ws('-', director_cooperate_actor.c.title), SEPARATOR="-"),
            sqlalchemy.func.count(director_cooperate_actor.c.movie_id),
        ) \
        .filter(director_cooperate_actor.c.actor_name.like("%{}%".format(actor))) \
        .group_by(
            director_cooperate_actor.c.director_id,
            director_cooperate_actor.c.actor_id,
            # director_cooperate_actor.c.movie_id
        ) \
        .having(sqlalchemy.func.count(director_cooperate_actor.c.movie_id) >= times)

    start_time = time.time()
    directors = directors_query.paginate(page=page, per_page=per_page).items
    consuming_time = time.time() - start_time

    return consuming_time, list(map(lambda x: {"name": x[0], "movies": x[1].split(','), "times": x[2]}, directors))
