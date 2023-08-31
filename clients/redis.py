# -*- coding: utf-8 -*-
import redis
from os import getenv


r = redis.StrictRedis(host=getenv('REDIS_HOST_CONNECT_URL'), port=6379, db=getenv('REDIS_DB_NUMBER'))

def set(id, data):
    r.set(id, data)

def get(id):
    return r.get(id)
