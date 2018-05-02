# coding=utf8
from process.fetch_cate_product import batch_product_ids, restore_cate_items_task
from process.update_category_true import JoomCategory
from sql.category import Category
from sql.task_schedule import TaskSchedule

__author__ = 'Administrator'

from func import get_joom_token
from utils.redis_util import redis_conn

import os
import traceback
from sqlalchemy import text
from functools import partial
from concurrent import futures
from sqlalchemy.sql import and_
from sql.base import db, sessionCM


#
# authorization = cc.get("joom#token")
#
#
def init_cate_task():
    with sessionCM() as session:
        cate_lst = session.query(Category.tag).filter(and_(Category.site_id == 31, Category.is_leaf == 1)).all()
        cate_lst = [ct[0] for ct in cate_lst]
        kwargs = {
            "kind": "cate",
            "site": 31,
            "keys": cate_lst
        }
        TaskSchedule.batch_insert(session, **kwargs)
    return True


#
def batch_cate_item_rev(auth):
    EXECUTOR = {
        "cate": batch_product_ids,
    }
    process_length = {
        "cate": 1,
        "item": 4,
        "rev": 4
    }
    for kind in ["cate"]:
        while True:
            print("kind: %s, begin a batch tasks @@@@@@" % kind)
            tasks = TaskSchedule.get_init_raw(kind, 31)
            if not tasks:
                if kind == "cate":
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


#
#
# def update_review_cnt():
#     print "update review count ..."
#     pro_sql = text("select pro_no from joom_pro;")
#     rev_cnt_sql = text("select count(*) from joom_review where pro_no=:pro_no;")
#     update_sql = text("update joom_pro set reviews_count=:reviews_count where pro_no=:pro_no;")
#     connect = db.connect()
#     pro_cursor = connect.execute(pro_sql)
#     for pro_item in pro_cursor:
#         tag = pro_item[0]
#         cnt_cursor = connect.execute(rev_cnt_sql, pro_no=tag)
#         cnt = cnt_cursor.fetchone()[0]
#         cnt_cursor.close()
#         up_cursor = connect.execute(update_sql, reviews_count=cnt, pro_no=tag)
#         up_cursor.close()
#     pro_cursor.close()
#     connect.close()


def self_killed():
    print "killed whole self process ..."
    cmd = "ps aux | grep 'andata' | awk '{print $2}' | xargs kill -9"
    os.system(cmd)


if __name__ == "__main__":
    import cProfile
    def begin ():
        redis_conn.delete("joom_token")
        auth = get_joom_token()
        will_update_category = raw_input("是否更新类目(y/n)?")
        if will_update_category.lower() in ["yes", "y"]:
            JoomCategory(auth).begin_stalk()
        will_clear_schedule = raw_input("是否清空任务队列(y/n)?")
        if will_clear_schedule.lower() in ["yes", "y"]:
            TaskSchedule.clear()
            init_cate_task()
        will_clear_before_pros = raw_input("是否清空原来的产品ID(y/n)?")
        if will_clear_before_pros.lower() in ["yes", "y"]:
            redis_conn.delete('cate#items')
        batch_cate_item_rev(auth)

    cProfile.run('begin()')
