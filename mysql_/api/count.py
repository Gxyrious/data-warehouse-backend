from flask import request, Blueprint, jsonify
from mysql_.model import *
import time, sqlalchemy
from collections import Counter

count = Blueprint("mysql_comprehensive_count", __name__)

@count.route('/movie', methods=['POST'])
def comprehensiveMoviePages():
    # 只需要传筛选条件和页码，不需要传columns
    data: dict = request.get_json()
    page, per_page = 1, 10
    try:
        page, per_page = int(data.pop("page")), int(data.pop("per_page"))
    except KeyError as e:
        print(e)
    # 获取筛选条件
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
            pass
    pages = db.session.query(t_movie_date_genre.c["movie_id"]).filter(*filters).paginate(page=page, per_page=per_page).pages
    return jsonify({"pages": pages})


@count.route('/relation', methods=['POST'])
def comprehensiveRelationPages():
    data: dict = request.get_json()
    name, times = data["name"], int(data["times"])
    source, target = data["source"], data["target"]
    page, per_page = 1, 10
    try:
        page, per_page = int(data.pop("page")), int(data.pop("per_page"))
    except KeyError as e:
        print(e)
    if source == "director" and target == "actor":
        pages = __getPagesOfActorCooperateWithDirector(name, times, page, per_page)
    elif source == "actor" and target == "actor":
        pages = __getPagesOfActorCooperateWithActor(name, times, page, per_page)
    elif source == "actor" and target == "director":
        pages = __getPagesOfDirectorCooperateWithActor(name, times, page, per_page)
    else:
        print(source, target)
    
    return jsonify({
        "pages": pages
    })

def __getPagesOfActorCooperateWithDirector(director, times, page, per_page):

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
    
    pages = actors_query.count() // per_page + 1

    return pages

def __getPagesOfActorCooperateWithActor(actor, times, page, per_page):

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
        ) \
        .filter(actor_cooperate_actor.c.right_actor_name.like("%{}%".format(actor)))
    right_query = db.session.query(
            # actor_cooperate_actor.c.right_actor_id.label("with_actor_id"),
            actor_cooperate_actor.c.right_actor_name.label("with_actor_name"),
        ) \
        .filter(actor_cooperate_actor.c.left_actor_name.like("%{}%".format(actor)))
    final_query = left_query.union_all(right_query)
    
    actors_all = final_query.all() # 查询
    delete_bracket = map(lambda x: x[0], actors_all) # 删除括号，取第一个
    count_occurrence_time = dict(Counter(delete_bracket)) # 计算出现次数，转换为字典
    time_filted = list(filter(lambda x: x[1] >= times, count_occurrence_time.items())) # 过滤
    print(time_filted)
    return len(list(time_filted)) // per_page + 1

def __getPagesOfDirectorCooperateWithActor(actor, times, page, per_page):

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

    pages = directors_query.count() // per_page + 1

    return pages