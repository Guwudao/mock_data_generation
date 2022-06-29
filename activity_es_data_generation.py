from elasticsearch import helpers
from elasticsearch import Elasticsearch


def connect_es():
    elasticsearch = Elasticsearch(
        ['http://120.25.104.37:9200']
    )
    return elasticsearch


def generate_es_data(data, info):
    actions = []
    for i in data:
        _id = i["id"]
        action = {
            "_index": 'activity_log',
            "_id": _id,
            "_source": i
        }
        actions.append(action)
    print(len(actions))
    # res, d = helpers.bulk(connect_es(), actions)
    print(info)
