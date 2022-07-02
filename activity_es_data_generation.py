from elasticsearch import helpers
from elasticsearch import Elasticsearch
import datetime


def get_time():
    t = datetime.datetime.now()
    now = t.strftime("%Y-%m-%d %H:%M:%S")
    return now


def connect_es():
    elasticsearch = Elasticsearch(
        ['http://120.25.104.37:9200']
    )
    return elasticsearch


def push_es_data(data, info):
    actions = []
    for i in data:
        _id = i["id"]
        action = {
            "_index": 'activity_log',
            "_id": _id,
            "_source": i
        }
        actions.append(action)
    helpers.bulk(connect_es(), actions)
    print(get_time() + " " + info)
