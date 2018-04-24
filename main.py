#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
 @Time    : 2018/4/23 17:24
 @Author  : jyq
 @Software: PyCharm
 @Description: 
"""
from concurrent import futures
from sqlalchemy import and_

from func import get_joom_token
from process.fetch_cate_product import batch_product_ids, restore_cate_items_task
from process.update_category import JoomCategory
from sql.category import Category
from sql.session import sessionCM
from sql.task_schedule import TaskSchedule
from utils.redis_util import redis_conn


def update_and_init_cate_task():
    with sessionCM() as session:
        cate_lst = session.query(Category.tag).filter(
            and_(Category.site_id == 31, Category.is_leaf == 1, Category.status == "0")).all()
        cate_lst = [ct[0] for ct in cate_lst]
        kwargs = {
            "kind": "cate",
            "site": 31,
            "keys": cate_lst
        }
        TaskSchedule.batch_upsert(session, **kwargs)
    return True


def batch_cate_item_rev(auth):
    EXECUTOR = {
        "cate": batch_product_ids,
    }
    for kind in ["cate"]:
        while True:
            tasks = TaskSchedule.get_raw_kind_batch(31, kind)
            if not tasks and TaskSchedule.is_raw_complete(kind):
                if kind == "cate" and not TaskSchedule.is_raw_starting("item"):
                    restore_cate_items_task()
                print("%s tasks all completed !!!" % kind)
                break
            fun_executor = EXECUTOR[kind]
            with futures.ThreadPoolExecutor(max_workers=64) as executor:
                future_to_worker = {
                    executor.submit(fun_executor, auth, **ts): ts for ts in tasks
                }
                for future in futures.as_completed(future_to_worker):
                    ts = future_to_worker[future]
                    try:
                        data = future.result()
                    except Exception as exc:
                        print("%s, kind: %s, generated an exception %s" % (ts, kind, exc))
            print("kind: %s, complete a batch tasks @@@@@@" % kind)


if __name__ == "__main__":
    redis_conn.delete("joom_token")
    auth = get_joom_token()
    will_update_cate = raw_input("是否需要更新类目(y/n)?")
    if will_update_cate.lower() in ["y", "yes"]:
        JoomCategory(auth).category()
    print u"正在重置任务状态"
    TaskSchedule.clear()
    redis_conn.delete("cate#items")
    print u"将分类添加到任务队列"
    update_and_init_cate_task()
    print u"正在采集分类下的产品列表"
    batch_cate_item_rev(auth)
