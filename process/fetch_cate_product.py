#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
 @Time    : 2018/4/24 10:15
 @Author  : jyq
 @Software: PyCharm
 @Description: 
"""
import traceback

import requests
from concurrent import futures

from func import random_key, get_joom_token

import ujson as json

#
# def fetch_cate_pro(token, cate_id, off=0):
#     url = 'https://api.joom.com/1.1/search/products?language=en-US&currency=USD_=jfrx%s' % random_key(4)
#
#     params = {
#         'count': 50,
#         'pageToken': 'off:%s' % off,
#         'filters': [{
#             'id': 'categoryId',
#             'value': {
#                 'type': 'categories',
#                 'items': [{
#                     'id': cate_id
#                 }]
#             }
#         }]
#     }
#     print(u"正在抓取分类%s下第%s-%s个产品" % (cate_id, off, off + 50))
#     res = requests.post(url, data=json.dumps(params), headers={
#         "authorization": token,
#         "content-type": 'application/json'
#     })
#     if "unauthorized" in res.content:
#         token = get_joom_token()
#         fetch_cate_pro(token, cate_id, off)
#         return
#     content = res.json()
#     items = content["payload"]["items"]
#     if len(items) == 0:
#         print(u"分类%s抓取完成!" % cate_id)
#     else:
#         for item in items:
#             print(u'产品id为%s' % item["id"])
#             fetch_review.delay(item["id"], token)
#         with futures.ThreadPoolExecutor(max_workers=4) as executor:
#             future_to_user = {
#                 executor.submit(fetch_pro, tag=item["id"], token=token): item["id"] for item in items
#             }
#             for future in futures.as_completed(future_to_user):
#                 rev_pro = future_to_user[future]
#                 try:
#                     rp = future.result()
#                 except Exception as exc:
#                     print("%s generated an exception: %s" % (rev_pro, exc))
#         fetch_cate_pro.delay(token, cate_id, off + 50)
from sql.task_schedule import TaskSchedule
from utils.redis_util import redis_conn


def batch_product_ids(self, **kwargs):
    pgToken = kwargs.get("value", None)
    cate = kwargs.get("key")
    if not pgToken:
        pgToken = None
        times = 0
    else:
        pgToken, times = pgToken.split("#")
        times = int(times)
    count = 48
    # 根据分类获取产品ID列表
    data = {
        "count": count,
        "filters": [
            {
                "id": "categoryId",
                "value": {
                    "type": "categories",
                    "items": [
                        {
                            "id": cate
                        }
                    ]
                }
            }
        ]
    }
    if pgToken:
        data["pageToken"] = pgToken
    data_str = json.dumps(data)
    url = self.batch_url % random_key(4)
    try:
        res = requests.post(url, data_str, headers=self.headers, timeout=15)
    except:
        res = requests.post(url, data_str, headers=self.headers, timeout=15)
    content = json.loads(res.content)
    if res.status_code == 200 and "payload" in content and times <= 10000 and "nextPageToken" in content["payload"]:
        items = content["payload"]["items"]
        items = [it["id"] for it in items]
        if items:
            redis_conn.sadd("cate#items", *items)
        if len(items) < count:
            result = TaskSchedule.raw_update(31, "cate", cate,
                                             value=content["payload"]["nextPageToken"] + "#" + str(times + 1),
                                             status=TaskSchedule.DONE)
        else:
            result = TaskSchedule.raw_update(31, "cate", cate,
                                             value=content["payload"]["nextPageToken"] + "#" + str(times + 1),
                                             status=TaskSchedule.INIT)
            if not result:
                print("cate update error with tag: %s" % cate)
                TaskSchedule.raw_set(31, "cate", cate, TaskSchedule.PEND)
        del items
    elif res.status_code == 200 and (
                        "payload" not in content or times > 10000 or "nextPageToken" not in content["payload"]):
        TaskSchedule.raw_set(31, "cate", cate, TaskSchedule.DONE)
    else:
        print("get cate products error: cate: %s, times: %s" % (cate, times))
        print(content)
        TaskSchedule.raw_set(31, "cate", cate, TaskSchedule.PEND)
    return True



def restore_cate_items_task():
    print("saving the cate items ...")
    with futures.ThreadPoolExecutor(max_workers=16) as executor:
        future_save_item = {
            executor.submit(raw_batch_save_item, s_item): s_item for s_item in
            redis_conn.sscan_iter("cate#items", count=300, batch=500)
        }
        for future in futures.as_completed(future_save_item):
            s_item = future_save_item[future]
            try:
                result = future.result()
            except Exception as exc:
                print("%r generated an exception: %s" % (s_item, exc))
    print("saved ok @@@")


def raw_batch_save_item(slice_items):
    try:
        if len(slice_items) == 0:
            return True
        TaskSchedule.raw_pure_upsert(slice_items, "item", 31)
        return True
    except:
        print(traceback.format_exc())
        return False