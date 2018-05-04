#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
 @Time    : 2018/5/3 21:01
 @Author  : jyq
 @Software: PyCharm
 @Description: 
"""

import gevent.pool
import ujson as json
import gevent.monkey
import time
from concurrent import futures
from sqlalchemy import text

from sql.base import db

gevent.monkey.patch_all()
import traceback

import requests
from geventhttpclient import HTTPClient
from geventhttpclient.url import URL

from func import get_joom_token, random_key
from sql.task_schedule import TaskSchedule


auth = get_joom_token()
product_url = "api.joom.com/1.1/products/%s?language=en-US&currency=USD&_=jfs7%s"
# http = HTTPClient('api.joom.com', port=443)
headers = {
    # "content-type": "application/json",
    "authorization": auth,
    "origin": "https://www.joom.com",
    "user-agent": "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.162 Safari/537.36"
}

i = 0

print headers
def get_pro(http, p_id):
    global i
    url = product_url % (p_id, random_key(4))
    try:
        # response = http.get(url, headers=headers)
        # print response.status_code
        response = requests.get("https://"+url, headers=headers)
        connect = db.connect()
        connect.execute(
            text("INSERT INTO test (name, value) VALUES (:name, :value)"),
            *[{'name': 1, 'value': 2},
              {'name': 3, 'value': 4}]
        )
        connect.close()
        # json.loads(response.read())
        i += 1
        print i
    except Exception, e:
        print traceback.format_exc(e)


# allow to run 20 greenlet at a time, this is more than concurrency
# of the http client but isn't a problem since the client has its own
# connection pool.
pool = gevent.pool.Pool(64)
tasks = TaskSchedule.get_init_raw('item', 31, limit=10000)


time1 = time.time()
for item in tasks:
    p_id = item['key']
    pool.add(gevent.spawn(get_pro, '', p_id))
    # get_pro(http, p_id)
pool.join()
# http.close()
time2 = time.time()

with futures.ThreadPoolExecutor(max_workers=64) as executor:
    future_to_worker = {}
    for item in tasks:
        future_to_worker[executor.submit(get_pro, '', item['key'])] = item

    for future in futures.as_completed(future_to_worker):
        ts = future_to_worker[future]
        try:
            data = future.result()
        except Exception as exc:
            print("%s, kind: %s, generated an exception %s" )
time3 = time.time()
print time2-time1
print time3-time2