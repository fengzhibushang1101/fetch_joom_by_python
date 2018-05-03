#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
 @Time    : 2018/5/3 11:35
 @Author  : jyq
 @Software: PyCharm
 @Description: 
"""
import traceback
import time
import datetime

import pickle
import requests
from concurrent import futures

from func import random_key, get_joom_token
import ujson as json

from sql.joom_pro import JoomPro
from sql.joom_shop import JoomShop
from sql.product_body import ProductBody
from sql.task_schedule import TaskSchedule
from utils.redis_util import redis_conn


class JoomProduct(object):
    def __init__(self, auth):
        self.batch_url = "https://api.joom.com/1.1/search/products?language=en-US&currency=USD&_=jfs3%s"
        self.product_url = "https://api.joom.com/1.1/products/%s?language=en-US&currency=USD&_=jfs7%s"
        self.auth = auth
        self.headers = {
            "content-type": "application/json",
            "authorization": auth,
            "origin": "https://www.joom.com"
        }


    def product_info(self, auth, db, **kwargs):
        # 产品详细信息
        pid = kwargs["key"]
        url = self.product_url % (pid, random_key(4))
        headers = self.headers.copy()
        headers["authorization"] = self.auth
        del headers["content-type"]
        try:
            res = requests.get(url, headers=headers, timeout=10)
            if "unauthorized" in res.content or "payload" not in res.content:
                self.auth = get_joom_token()
                res = requests.get(url, headers=headers, timeout=10)
        except:
            try:
                res = requests.get(url, headers=headers, timeout=10)
            except:
                TaskSchedule.raw_set(31, "item", pid, TaskSchedule.ERROR, 1, _db=db)
                return False
        content = json.loads(res.content)
        if "payload" not in content:
            print("tag: %s, payload not in content: %s" % (pid, content))
            TaskSchedule.raw_set(31, "item", pid, TaskSchedule.ERROR, 1, _db=db)
            return True
        pro_body, shop_info, pro_info = self.trans_pro(content)
        connect = db.connect()
        self.save_body(connect, **pro_body)
        connect.close()
        redis_conn.sadd("joom_items#%s" % kwargs["pid"], pickle.dumps(pro_info))
        redis_conn.sadd("joom_shops#%s" % kwargs["pid"], pickle.dumps(shop_info))
        TaskSchedule.raw_set(31, "item", pid, TaskSchedule.DONE, 1, _db=db)

        return True

    def trans_pro(self, res):
        pro_data = dict()
        item = res["payload"]
        tag = item["id"]
        shop_data = item["store"]
        shop_info = {
            "name": shop_data["name"],
            "shop_no": shop_data["id"],
            "logo": "" if not shop_data.get("image") else shop_data["image"]["images"][3]["url"],
            "rate": shop_data.get("rating", 0),
            "is_verify": "1" if shop_data["verified"] else "0",
            "save_count": shop_data["favoritesCount"]["value"],
            "create_time": datetime.datetime.fromtimestamp(shop_data[
                                                               "updatedTimeMerchantMs"] / 1000) if "updatedTimeMerchantMs" in shop_data else datetime.datetime.strptime(
                "1997-01-01 00:00:00", "%Y-%m-%d %H:%M:%S"),
            "update_time": datetime.datetime.fromtimestamp(shop_data[
                                                               "updatedTimeMerchantMs"] / 1000) if "updatedTimeMerchantMs" in shop_data else datetime.datetime.strptime(
                "1997-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")
        }
        pro_info = {
            "name": item["name"],
            "pro_no": item["id"],
            "shop_no": item["storeId"],
            "category_id": item.get("categoryId", "0"),
            "image": item["mainImage"]["images"][3]["url"] if "mainImage" in item else "",
            "rate": item.get("rating", 0),
            "msrp": item["lite"].get("msrPrice", 0),
            "discount": item["lite"].get("discount", 0),
            "real_price": item["lite"]["price"],
            "reviews_count": item["reviewsCount"]["value"],
            "create_time": datetime.datetime.fromtimestamp(min(map(lambda z: z["createdTimeMs"], item[
                "variants"])) / 1000) if "variants" in item else datetime.datetime.strptime("1997-01-01 00:00:00",
                                                                                            "%Y-%m-%d %H:%M:%S"),
            "update_time": datetime.datetime.fromtimestamp(max(map(lambda z: z["publishedTimeMs"], item[
                "variants"])) / 1000) if "variants" in item else datetime.datetime.strptime("1997-01-01 00:00:00",
                                                                                            "%Y-%m-%d %H:%M:%S")
            # 这里不准确
        }
        pro_info["r_count_30"] = pro_info["r_count_7"] = pro_info["r_count_7_14"] = pro_info["growth_rate"] = 0
        parent_info = item["lite"]
        pro_data["SourceInfo"] = {
            "Platform": "Joom",
            "Link": "https://www.joom.com/en/products/%s/" % tag,
            "Site": "Global",
            "SiteID": 31,
            "ProductID": tag
        }
        pro_data["Title"] = item["name"]
        pro_data["rating"] = item.get("rating", 0)
        pro_data["reviews_count"] = item["reviewsCount"]
        pro_data["store_id"] = item["storeId"]
        pro_data["keywords"] = item.get("tags", [])
        pro_data["price"] = parent_info["price"]
        pro_data["MSRP"] = parent_info.get("msrPrice", 0)
        pro_data["discount"] = parent_info["discount"]
        pro_data["Description"] = item["description"]
        pro_data["ProductSKUs"] = list()
        pro_data["images"] = self.get_images(item)
        pro_data["ProductSKUs"] = self.get_variants(item["variants"]) if "variants" in item else []
        pro_body = {"key": item["id"], "site": 31, "body": json.dumps(pro_data)}
        return pro_body, shop_info, pro_info

    def get_images(self, item):
        try:
            extra_images = [image["payload"]["images"][3]["url"] for image in item["gallery"] if
                            image["type"] == "image"]
        except:
            extra_images = []
        main_image = item["mainImage"]["images"][3]["url"] if "mainImage" in item else ""
        return [main_image] + extra_images

    def get_variants(self, variations):
        pro_vars = []
        for variation in variations:
            v_specifics = []
            if variation.get("colors"):
                v_specifics.append({
                    "Image": [],
                    "ValueID": variation["colors"][0].get("rgb", ""),
                    "NameID": "",
                    "Name": "Color",
                    "Value": variation["colors"][0]["name"]
                })
            if variation.get("size"):
                v_specifics.append({
                    "Image": [],
                    "ValueID": "",
                    "NameID": "",
                    "Name": "Size",
                    "Value": variation["size"]
                })
            pro_vars.append({
                "SkuID": variation["id"],  # 变体SKUID
                "PictureURL": "" if not variation.get("mainImage") else variation["mainImage"]["images"][3]["url"],
                "Active": variation["inStock"],
                "VariationSpecifics": v_specifics,  # 变体属性信息
                "Price": variation["price"],  # 价格
                "ShippingTime": "%s-%s" % (variation["shipping"]["maxDays"], variation["shipping"]["minDays"]),  # 运送时间
                "ShippingCost": variation["shipping"]["price"],  # 运费
                "MSRP": variation.get("msrPrice", 0),  # msrp
                "Stock": variation["inventory"]  # 库存
            })
        return pro_vars

    def save_product(self, connect, **product):
        try:
            JoomPro.raw_upsert(connect, **product)
            return True
        except:
            print(traceback.format_exc())
            return False

    def save_body(self, connect, **body):
        try:
            ProductBody.raw_upsert(connect, **body)
            return True
        except:
            print(traceback.format_exc())
            return False

    def save_shop(self, connect, **shop):
        try:
            JoomShop.raw_upsert(connect, **shop)
            return True
        except:
            print(traceback.format_exc())
            return False

    @classmethod
    def batch_save_pro(cls, connect, infos):
        infos = map(lambda x: pickle.loads(x), infos)
        try:
            JoomPro.batch_upsert(connect, infos)
        except:
            print(traceback.format_exc())
            return False

    @classmethod
    def batch_save_shop(cls, connect, infos):
        infos = map(lambda x: pickle.loads(x), infos)
        try:
            JoomShop.batch_upsert(connect, infos)
        except:
            print(traceback.format_exc())
            return False

    @classmethod
    def add_pro_to_mysql(cls, connect, i):
        with futures.ThreadPoolExecutor(max_workers=4) as executor:
            future_save_item = {
                executor.submit(cls.batch_save_pro, connect, s_item): s_item for s_item in
                redis_conn.sscan_iter("joom_items#%s" % i, count=4, batch=2500)
            }
            for future in futures.as_completed(future_save_item):
                s_item = future_save_item[future]
                try:
                    result = future.result()
                except Exception as exc:
                    print("%r generated an exception: %s" % (s_item, exc))
        redis_conn.delete("joom_items#%s" % i)
        print("saved ok @@@")

    @classmethod
    def add_shop_to_mysql(cls, connect, i):
        with futures.ThreadPoolExecutor(max_workers=4) as executor:
            future_save_item = {
                executor.submit(cls.batch_save_shop, connect, s_item): s_item for s_item in
                redis_conn.sscan_iter("joom_shops#%s" % i, count=4, batch=2500)
            }
            for future in futures.as_completed(future_save_item):
                s_item = future_save_item[future]
                try:
                    result = future.result()
                except Exception as exc:
                    print("%r generated an exception: %s" % (s_item, exc))
        redis_conn.delete("joom_shops#%s" % i)
        print("saved ok @@@")
