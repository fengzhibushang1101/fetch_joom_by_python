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
from sql.task_schedule import TaskSchedule
from utils.redis_util import redis_conn

batch_url = "https://api.joom.com/1.1/search/products?language=en-US&currency=USD&_=jfs3%s"


def batch_product_ids(auth, **kwargs):
    headers = {
        "content-type": "application/json",
        "authorization": auth,
        "origin": "https://www.joom.com"
    }
    pg_token = kwargs.get("next_token", None)
    cate = kwargs["key"]
    times = kwargs.get("dealtime", 0)
    error_times = kwargs.get("error_times", 0)

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
    if pg_token:
        data["pageToken"] = pg_token
    data_str = json.dumps(data)
    url = batch_url % random_key(4)
    try:
        res = requests.post(url, data_str, headers=headers, timeout=15)
    except Exception, e:
        error_times += 1
        error_status = TaskSchedule.INIT
        if error_times > 3:
            print("get cate products error: cate: %s, times: %s" % (cate, times))
            print(e.message)
            error_status = TaskSchedule.ERROR
        TaskSchedule.raw_set(31, "cate", cate, error_status, times, error_times, pg_token)
        return
    if "unauthorized" in res.content:
        auth = get_joom_token()
        batch_product_ids(auth, **kwargs)

    content = res.json()
    if res.status_code == 200 and "payload" in content and times <= 10000 and "nextPageToken" in content["payload"]:
        items = content["payload"]["items"]
        items = [it["id"] for it in items]
        if items:
            redis_conn.sadd("cate#items", *items)
        if len(items) == 0:
            result = TaskSchedule.raw_set(31, "cate", cate, TaskSchedule.DONE, times)
        else:
            result = TaskSchedule.raw_set(31, "cate", cate, TaskSchedule.INIT, times+1, 0, content["payload"]["nextPageToken"])
            if not result:
                print("cate update error with tag: %s" % cate)
                TaskSchedule.raw_set(31, "cate", cate, TaskSchedule.PEND, times, error_times+1, pg_token)
        del items
    elif res.status_code == 200 and (
                        "payload" not in content or times > 10000 or "nextPageToken" not in content["payload"]):
        TaskSchedule.raw_set(31, "cate", cate, TaskSchedule.DONE, times, 0)
    else:
        error_times += 1
        error_status = TaskSchedule.INIT
        if error_times > 3:
            error_status = TaskSchedule.ERROR
            print("get cate products error: cate: %s, times: %s" % (cate, times))
            print(content)
        TaskSchedule.raw_set(31, "cate", cate, error_status, times, error_times, pg_token)
    return True


def restore_cate_items_task():
    print("saving the cate items ...")
    with futures.ThreadPoolExecutor(max_workers=64) as executor:
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
