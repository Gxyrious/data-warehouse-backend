from flask import request, Blueprint, jsonify
from neo4j_.model import graph
from py2neo import Graph, Node, Relationship, NodeMatcher, RelationshipMatcher
import time

comprehensive = Blueprint("neo4j_comprehensive", __name__)


# http://81.68.102.171:5000/neo4j/bycomprehensive/change-your-route-here
@comprehensive.route('/relation', methods=['POST'])
def neo4j_comprehensive_movie():
    # get data from post
    # 根据输入的方法选择合作类型，以下为演员和演员
    conditions = request.get_json()
    a = conditions.get('source')
    b = conditions.get('target')
    times = conditions.get('times')
    inname = conditions.get('name')
    print(a)
    print(b)
    print(conditions)
    result = {'data': {}}
    # calculate time
    start_time = time.time()
    if a == 'actor' and b == 'actor':
        node_matcher = NodeMatcher(graph)
        relationship_matcher = RelationshipMatcher(graph)
        node1 = node_matcher.match('actor').where(name=inname).first()  # aid可以改为name，输入source的name
        filnum = "_.num>=" + str(times)  # 5为测试数据 改为times
        relationship = list(relationship_matcher.match([node1], r_type='actope').where(filnum).all())
        print(relationship)
        result.update({'data': relationship})
        # ..
    elif a == 'director' and b == 'actor':
        node_matcher = NodeMatcher(graph)
        relationship_matcher = RelationshipMatcher(graph)
        node1 = node_matcher.match('director').where(name=inname).first()
        filnum = "_.num>=" + str(times)  # 5为测试数据 改为times
        relationship = list(relationship_matcher.match([node1], r_type='directope').where(filnum).all())
        print(relationship)
        result.update({'data': relationship})
        # ..
    elif a == 'actor' and b == 'director':
        node_matcher = NodeMatcher(graph)
        relationship_matcher = RelationshipMatcher(graph)
        node1 = node_matcher.match('actor').where(name=inname).first()
        filnum = "_.num>=" + str(times)  # 5为测试数据 改为times
        relationship = list(relationship_matcher.match([node1], r_type='adirectope').where(filnum).all())
        print(relationship)
        result.update({'data': relationship})
        # ..
    end_time = time.time()
    consuming_time = end_time - start_time
    result.update({'consuming_time': consuming_time})
    return jsonify(result)
