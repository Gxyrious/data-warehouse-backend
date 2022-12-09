from flask import request, Blueprint
from sqlalchemy import func, or_, orm
from mysql.model import *
from collections import Counter

cooperation = Blueprint("cooperation", __name__)

@cooperation.route('/with_actor/director', methods=['GET'])
def getActorCooperateWithDirector():
    data = request.args
    director, time = data.get('director'), int(data.get('time'))
    # 查找与director合作次数超过time的actor
    director_cooperate_actor = db.session.query(
            Director.name.label("director_name"), 
            Cooperation.left_person_id.label("director_id"), 
            # Director.director_id.label("director_id"),
            Actor.name.label("actor_name"), 
            Cooperation.right_person_id.label("actor_id"), 
            # Actor.actor_id.label("actor_id"), 
            Cooperation.movie_id.label("movie_id")
        ) \
        .filter(
            Cooperation.type == 1, 
            Director.director_id == Cooperation.left_person_id, 
            Actor.actor_id == Cooperation.right_person_id
        ) \
        .subquery()
    # 子查询，subquery.c表示从自定的Column列中找
    actors = db.session.query(director_cooperate_actor.c.actor_name) \
        .filter(director_cooperate_actor.c.director_name.like("%{}%".format(director))) \
        .group_by(
            director_cooperate_actor.c.director_id,
            director_cooperate_actor.c.actor_id
        ) \
        .having(func.count(director_cooperate_actor.c.movie_id) >= time) \
        .all()
    return str(list(map(lambda x: x[0], actors)))


@cooperation.route('/with_actor/actor', methods=['GET'])
def getActorCooperateWithActor():
    data = request.args
    actor, time = data.get('actor'), int(data.get('time'))

    # 查找与actor合作次数超过time的actor
    left_actors: Actor = orm.aliased(Actor)
    right_actors: Actor = orm.aliased(Actor)

    actor_cooperate_actor = db.session.query(
            left_actors.name.label("left_actor_name"),
            Cooperation.left_person_id.label("left_actor_id"),
            # left_actors.actor_id.label("left_actor_id"),
            right_actors.name.label("right_actor_name"),
            Cooperation.right_person_id.label("right_actor_id"),
            # right_actors.actor_id.label("right_actor_id"),
            Cooperation.movie_id.label("movie_id")
        ) \
        .filter(
            Cooperation.type == 2,
            left_actors.actor_id == Cooperation.left_person_id,
            right_actors.actor_id == Cooperation.right_person_id
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

    # actors_all = final_query.all() # 查询
    # delete_bracket = map(lambda x: x[0], actors_all) # 删除括号，取第一个
    # count_occurrence_time = dict(Counter(delete_bracket)) # 计算出现次数，转换为字典
    # time_filted = filter(lambda x: x[1] >= time, count_occurrence_time.items()) # 过滤
    # get_actor_name = list(map(lambda x: x[0], time_filted)) # 获取姓名
    # return str(get_actor_name)

    return str(list(map(lambda x: x[0], filter(lambda x: x[1] >= time, dict(Counter(map(lambda x: x[0], final_query.all()))).items()))))


@cooperation.route('/with_director/actor', methods=['GET'])
def getDirectorCooperateWithActor():
    data = request.args
    actor, time = data.get('actor'), int(data.get('time'))
    # 查找与actor合作次数超过time的director
    director_cooperate_actor = db.session.query(
            Director.name.label("director_name"),
            Cooperation.left_person_id.label("director_id"),
            # Director.director_id.label("director_id"),
            Actor.name.label("actor_name"), 
            Cooperation.right_person_id.label("actor_id"),
            # Actor.actor_id.label("actor_id"),
            Cooperation.movie_id.label("movie_id"),
        ) \
        .filter(
            Cooperation.type == 1,
            Director.director_id == Cooperation.left_person_id,
            Actor.actor_id == Cooperation.right_person_id
        ) \
        .subquery()
    print(type(director_cooperate_actor))
    directors = db.session.query(director_cooperate_actor.c.director_name) \
        .filter(director_cooperate_actor.c.actor_name.like("%{}%".format(actor))) \
        .group_by(
            director_cooperate_actor.c.director_id,
            director_cooperate_actor.c.actor_id
        ) \
        .having(func.count(director_cooperate_actor.c.movie_id) >= time) \
        .all()

    return str(list(map(lambda x: x[0], directors)))
