# -*- coding: utf-8 -*-
import redis


r = redis.StrictRedis(host='localhost', port=6379, db=0)

def set(id, data):
    r.set(id, data)

def get(id):
    return r.get(id)
