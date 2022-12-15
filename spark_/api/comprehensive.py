# comprehensive search
from flask import request, Blueprint, jsonify
import time
from spark_.utils import get_spark_session

comprehensive = Blueprint("spark_comprehensive", __name__)


@comprehensive.route('/movie', methods=['POST'])
def spark_comprehensive_movie():
    # time
    consuming_time = 0

    # get data from post
    data: dict = request.get_json()
    data = {k: v for k, v in data.items() if v is not None}

    print(data)
    columns = ["score", "edition", "date", "genre_name", "title","asin","actors","directors"]
    columns = data.pop('columns')

    page, per_page = 1, 10
    try:
        page, per_page = int(data.pop("page")), int(data.pop("per_page"))
    except KeyError as e:
        print(e)

    session = get_spark_session()
    t_movie_date_genre = session.sql('select * from movie_date_genre')
    t_movie_format = session.sql('select * from movie_format')
    t_movie_actor = session.sql('select * from movie_actor')
    t_movie_director = session.sql('select * from movie_director')
    t_movie_asin = session.sql('select * from asin')

    # t_all=t_movie_date_genre.join(
    #     t_movie_format,['movie_id'],'fullouter').join(
    #     t_movie_actor,['movie_id'],'fullouter').join(
    #     t_movie_director,['movie_id'],'fullouter').join(
    #     t_movie_asin,['movie_id'],'fullouter'
    #     )

    simple_columns = list(
        {"score", "edition", "date", "genre_name", "title"}.intersection(set(columns)))
    if "date" in simple_columns:
        simple_columns.remove("date")
        simple_columns.append("movie_date")

    simple_columns.append("movie_id")

    filters = []

    filter_movie_id_list: set = set()

    for key, value in data.items():
        print(key, value)
        if key == "genre_name":
            filters.append(t_movie_date_genre["genre_name"] == value)
        elif key == "title":
            filters.append(t_movie_date_genre["title"].like(
                "%{}%".format(value)))
        elif key == "format":
            start_time = time.time()
            format_list = [row['movie_id'] for row in t_movie_format.filter(
                t_movie_format['format_name'].like("%{}%".format(value))).collect()]
            if len(filter_movie_id_list) != 0:
                filter_movie_id_list.intersection(set(format_list))
            else:
                filter_movie_id_list = set(format_list)
            consuming_time += time.time() - start_time
        elif key == "actor":
            start_time = time.time()
            actor_list = [row['movie_id'] for row in t_movie_actor.filter(
                t_movie_actor['actor_name'].like("%{}%".format(value))).collect()]
            if len(filter_movie_id_list) != 0:
                filter_movie_id_list.intersection(set(actor_list))
            else:
                filter_movie_id_list = set(actor_list)
            consuming_time += time.time() - start_time
        elif key == "director":
            start_time = time.time()
            director_list = set([row['movie_id'] for row in t_movie_director.filter(
                t_movie_director['director_name'].like("%{}%".format(value))).collect()])
            if len(filter_movie_id_list) != 0:
                filter_movie_id_list.intersection(set(director_list))
            else:
                filter_movie_id_list = set(director_list)
            consuming_time += time.time() - start_time
        elif key == "min_score":
            filters.append(t_movie_date_genre["score"] >= value)
        elif key == "max_score":
            filters.append(t_movie_date_genre["score"] <= value)
        elif key in ("year", "month", "day", "season", "weekday"):
            filters.append(t_movie_date_genre[key] == value)
        else:
            print(key, value)

    if len(filter_movie_id_list) != 0:
        filter_movie_id_list = list(set(filter_movie_id_list))

    result_of_simple_columns = t_movie_date_genre

    for f in filters:
        print(f)
        start_time = time.time()
        result_of_simple_columns = result_of_simple_columns.filter(f)
        consuming_time += time.time() - start_time
        result_of_simple_columns.show()
        if result_of_simple_columns.count() == 0:
            return jsonify({
                "count": count,
                "consuming_time": consuming_time,
                "data": result,
            })

    result_of_simple_columns = result_of_simple_columns.select(simple_columns)

    result = []

    end = page * per_page
    count = result_of_simple_columns.count()
    if end > count:
        end = count

    r = result_of_simple_columns.toPandas()
    if len(filter_movie_id_list) != 0:
        r = r[r['movie_id'].isin(filter_movie_id_list)]

    print(r)

    if r.shape[0] == 0:
        return jsonify({
            "count": count,
            "consuming_time": consuming_time,
            "data": [],
        })

    for i, row in r.iloc[(page-1)*per_page:end, :].iterrows():
        single_result = {}
        for index in range(len(simple_columns)):
            if simple_columns[index] == "movie_id":
                continue
            elif simple_columns[index] == "movie_date":
                datetime = row[index]
                single_result['movie_date'] = str(
                    datetime).split(' ')[0]
            else:
                single_result[simple_columns[index]] = row[index]
        movie_id = row[-1]
        print("movie_id = ", movie_id)
        if "actors" in columns:
            start_time = time.time()
            actors = t_movie_actor \
                .filter(t_movie_actor["movie_id"] == movie_id) \
                .select("actor_name").rdd.flatMap(lambda x: x).collect()
            consuming_time += time.time() - start_time
            print("actors = ", actors)
            single_result["actors"] = actors
        if "directors" in columns:
            start_time = time.time()
            directors = t_movie_director \
                .filter(t_movie_director["movie_id"] == movie_id) \
                .select("director_name").rdd.flatMap(lambda x: x).collect()
            consuming_time += time.time() - start_time
            print("directors = ", directors)
            single_result["directors"] = directors
        if "format" in columns:
            start_time = time.time()
            formats = t_movie_format \
                .filter(t_movie_format["movie_id"] == movie_id) \
                .select("format_name").rdd.flatMap(lambda x: x).collect()
            consuming_time += time.time() - start_time
            print("formats = ", formats)
            single_result["formats"] = list(map(lambda x: x[0], formats))
        if "asin" in columns:
            start_time = time.time()
            asins = t_movie_asin \
                .filter(t_movie_asin["movie_id"] == movie_id) \
                .select("asin").rdd.flatMap(lambda x: x).collect()
            consuming_time += time.time() - start_time
            print("asins = ", asins)
            single_result["asin"] = asins
        result.append(single_result)

    print(result)

    return jsonify({
        "count": count,
        "consuming_time": consuming_time,
        "data": result,
    })

   # result['data'] = df.rdd.map(lambda row: row.asDict()).collect()


@comprehensive.route('/relation', methods=['POST'])
def spark_relation():
    # time
    consuming_time = 0

    # get data from post
    data = request.get_json()
    data = {k: v for k, v in data.items() if v is not None}
    print(data)

    name, times = data["name"], int(data["times"])
    source, target = data["source"], data["target"]
    page, per_page = 1, 10
    try:
        page, per_page = int(data.pop("page")), int(data.pop("per_page"))
    except KeyError as e:
        print(e)
    if source == "director" and target == "actor":
        consuming_time, result = __getActorCooperateWithDirector(
            name, times, page, per_page)
    elif source == "actor" and target == "actor":
        consuming_time, result = __getActorCooperateWithActor(
            name, times, page, per_page)
    elif source == "actor" and target == "director":
        consuming_time, result = __getDirectorCooperateWithActor(
            name, times, page, per_page)
    else:
        print(source, target)

    return jsonify({
        "count": len(result),
        "consuming_time": consuming_time,
        "data": result
    })


def __getActorCooperateWithDirector(director, times, page, per_page):
    consuming_time = 0
    session = get_spark_session()

    # 查找与director合作次数超过time的actor
    director_cooperate_actor = session.sql(
        '''select
        a.name name,
        count(c.movie_id) times,
        collect_list(m.title) title
        from
        cooperation c join
        director d on c.left_person_id = d.director_id join
        actor a on c.right_person_id = a.actor_id
        join movie m on c.movie_id = m.movie_id
        where type = 1''' + " and d.name like '%{}%'".format(director, times) +
        ''' group by d.name, c.left_person_id, a.name, c.right_person_id 
            having count(c.movie_id) > {}'''.format(times)
    )

    director_cooperate_actor.show()

    start_time = time.time()
    result = director_cooperate_actor.rdd.map(
        lambda row: row.asDict()).collect()
    consuming_time += time.time()-start_time

    end = page * per_page
    if end > len(result):
        end = len(result)

    return consuming_time, result[(page-1)*per_page:end]


def __getActorCooperateWithActor(actor, times, page, per_page):
    consuming_time = 0
    session = get_spark_session()

    # 查找与actor合作次数超过time的actor(left)
    actor1 = session.sql(
        '''select
        righta.name name,
        count(c.movie_id) times,
        collect_list(m.title) title
        from
        cooperation c join
        actor lefta on c.left_person_id = lefta.actor_id join
        actor righta on c.right_person_id = righta.actor_id
        join movie m on c.movie_id = m.movie_id
        where type = 2''' + " and lefta.name like '%{}%'".format(actor) +
        ''' group by lefta.name, c.left_person_id, righta.name, c.right_person_id 
            having count(c.movie_id) > {}'''.format(times)
    )

    actor1.show()

    start_time = time.time()
    result = actor1.rdd.map(lambda row: row.asDict()).collect()
    consuming_time += time.time()-start_time

    # 查找与actor合作次数超过time的actor(right)
    actor2 = session.sql(
        '''select
        lefta.name name,
        count(c.movie_id) times,
        collect_list(m.title) title
        from
        cooperation c join
        actor lefta on c.left_person_id = lefta.actor_id join
        actor righta on c.right_person_id = righta.actor_id
        join movie m on c.movie_id = m.movie_id
        where type = 2''' + " and righta.name like '%{}%'".format(actor) +
        ''' group by lefta.name, c.left_person_id, righta.name, c.right_person_id 
            having count(c.movie_id) > {}'''.format(times)
    )

    actor2.show()

    start_time = time.time()
    result += actor2.rdd.map(lambda row: row.asDict()).collect()
    consuming_time += time.time()-start_time

    end = page * per_page
    if end > len(result):
        end = len(result)

    return consuming_time, result[(page-1)*per_page:end]


def __getDirectorCooperateWithActor(actor, times, page, per_page):
    consuming_time = 0
    session = get_spark_session()

    # 查找与actor合作次数超过time的director
    actor_cooperate_director = session.sql(
        '''select
        d.name name,
        count(c.movie_id) times,
        collect_list(m.title) title
        from
        cooperation c join
        director d on c.left_person_id = d.director_id join
        actor a on c.right_person_id = a.actor_id
        join movie m on c.movie_id = m.movie_id
        where type = 1''' + " and a.name like '%{}%'".format(actor, times) +
        ''' group by d.name, c.left_person_id, a.name, c.right_person_id 
            having count(c.movie_id) > {}'''.format(times)
    )

    actor_cooperate_director.show()

    start_time = time.time()
    result = actor_cooperate_director.rdd.map(
        lambda row: row.asDict()).collect()
    consuming_time += time.time()-start_time

    end = page * per_page
    if end > len(result):
        end = len(result)

    return consuming_time, result[(page-1)*per_page:end]
