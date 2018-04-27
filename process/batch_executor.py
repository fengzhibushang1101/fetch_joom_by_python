#coding=utf8
from process.update_category_true import JoomCategory

__author__ = 'Administrator'

from func import get_joom_token
from utils.redis_util import redis_conn

import os
import traceback
from sqlalchemy import text
from functools import partial
from concurrent import futures
from sqlalchemy.sql import and_
from sql.base import db

#
# authorization = cc.get("joom#token")
#
#
# def update_and_init_cate_task():
#     jc = JoomCategory(authorization)
#     jc.category()
#     with sessionCM() as session:
#         cate_lst = session.query(Category.tag).filter(and_(Category.site_id==31, Category.is_leaf==1, Category.status=="0")).all()
#         cate_lst = [ct[0] for ct in cate_lst]
#         kwargs = {
#             "kind": "cate",
#             "site": 31,
#             "keys": cate_lst
#         }
#         TaskSchedule.batch_upsert(session, **kwargs)
#     return True
#
#
# def batch_cate_item_rev():
#     jp = JoomProduct(authorization)
#     jr = JoomRev(authorization)
#     EXECUTOR = {
#         "cate": jp.batch_product_ids,
#         "item": jp.product_info,
#         "rev": jr.crawl_review
#     }
#     for kind in ["cate", "item", "rev"]:
#         while True:
#             tasks = TaskSchedule.get_raw_kind_batch(31, kind)
#             if not tasks and TaskSchedule.is_raw_complete(kind):
#                 if kind == "cate" and not TaskSchedule.is_raw_starting("item"):
#                     jp.restore_cate_items_task()
#                 print("%s tasks all completed !!!" % kind)
#                 break
#             fun_executor = EXECUTOR[kind]
#             with futures.ThreadPoolExecutor(max_workers=64) as executor:
#                 future_to_worker = {
#                     executor.submit(fun_executor, **ts): ts for ts in tasks
#                 }
#                 for future in futures.as_completed(future_to_worker):
#                     ts = future_to_worker[future]
#                     try:
#                         data = future.result()
#                     except Exception as exc:
#                         logger.error("%s, kind: %s, generated an exception %s" % (ts, kind, exc))
#             print("kind: %s, complete a batch tasks @@@@@@" % kind)
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
    
    redis_conn.delete("joom_token")
    auth = get_joom_token()
    JoomCategory(auth).begin_stalk()
    # self_killed()