# coding=utf8
__author__ = 'Administrator'
import requests
import datetime
import traceback
import ujson as json
from concurrent import futures
from andata.lib.model.base import db
from andata.lib.utils.logger import logger
from andata.lib.nosql.redis_client import cc
from andata.lib.utils.base import random_key
from andata.lib.model.joom_pro import JoomPro
from andata.lib.model.joom_shop import JoomShop
from andata.controls.joom_token import joom_token
from andata.lib.model.product_body import ProductBody
from andata.lib.model.task_schedule import TaskSchedule


class JoomProduct(object):
    def __init__(self, auth):
        self.batch_url = "https://api.joom.com/1.1/search/products?language=en-US&currency=USD&_=jfs3%s"
        self.product_url = "https://api.joom.com/1.1/products/%s?language=en-US&currency=USD&_=jfs7%s"
        self.headers = {
            "content-type": "application/json",
            "authorization": auth,
            "origin": "https://www.joom.com"
        }

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
                cc.sadd("cate#items", *items)
            if len(items) < count:
                result = TaskSchedule.raw_update(31, "cate", cate,
                                                 value=content["payload"]["nextPageToken"] + "#" + str(times + 1),
                                                 status=TaskSchedule.DONE)
            else:
                result = TaskSchedule.raw_update(31, "cate", cate,
                                                 value=content["payload"]["nextPageToken"] + "#" + str(times + 1),
                                                 status=TaskSchedule.INIT)
                if not result:
                    logger.error("cate update error with tag: %s" % cate)
                    TaskSchedule.raw_set(31, "cate", cate, TaskSchedule.PEND)
            del items
        elif res.status_code == 200 and (
                    "payload" not in content or times > 10000 or "nextPageToken" not in content["payload"]):
            TaskSchedule.raw_set(31, "cate", cate, TaskSchedule.DONE)
        else:
            logger.error("get cate products error: cate: %s, times: %s" % (cate, times))
            logger.error(content)
            TaskSchedule.raw_set(31, "cate", cate, TaskSchedule.PEND)
        return True

    def raw_batch_save_item(self, slice_items):
        try:
            if len(slice_items) == 0:
                return True
            TaskSchedule.raw_pure_upsert(slice_items, "item", 31)
            return True
        except:
            logger.error(traceback.format_exc())
            return False

    def restore_cate_items_task(self):
        logger.debug("saving the cate items ...")
        with futures.ThreadPoolExecutor(max_workers=32) as executor:
            future_save_item = {
                executor.submit(self.raw_batch_save_item, s_item): s_item for s_item in
            cc.sscan_iter("cate#items", count=300, batch=500)
            }
            for future in futures.as_completed(future_save_item):
                s_item = future_save_item[future]
                try:
                    result = future.result()
                except Exception as exc:
                    logger.error("%r generated an exception: %s" % (s_item, exc))
        logger.debug("saved ok @@@")

    def product_info(self, **kwargs):
        # 产品详细信息
        pid = kwargs["key"]
        url = self.product_url % (pid, random_key(4))
        headers = self.headers.copy()
        del headers["content-type"]
        try:
            res = requests.get(url, headers=headers, timeout=10)
            if "unauthorized" in res.content or "payload" not in res.content:
                joom_token()
                res = requests.get(url, headers=headers, timeout=10)
        except:
            try:
                res = requests.get(url, headers=headers, timeout=10)
            except:
                TaskSchedule.raw_set(31, "item", pid, TaskSchedule.PEND)
                return False
        content = json.loads(res.content)
        if "payload" not in content:
            logger.error("tag: %s, payload not in content: %s" % (pid, content))
            TaskSchedule.raw_set(31, "item", pid, TaskSchedule.PEND)
            return True
        pro_body, shop_info, pro_info = self.trans_pro(content)
        connect = db.connect()
        if pro_info["reviews_count"] and (
                pro_info["reviews_count"] > 99 or (pro_info["reviews_count"] > JoomPro.pro_review_cnt(pid))):
            TaskSchedule.raw_upsert(connect, pid, "rev", 31)
        self.save_body(connect, **pro_body)
        self.save_product(connect, **pro_info)
        self.save_shop(connect, **shop_info)
        TaskSchedule.raw_set(31, "item", pid, TaskSchedule.DONE)
        connect.close()
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
            logger.error(traceback.format_exc())
            return False

    def save_body(self, connect, **body):
        try:
            ProductBody.raw_upsert(connect, **body)
            return True
        except:
            logger.error(traceback.format_exc())
            return False

    def save_shop(self, connect, **shop):
        try:
            JoomShop.raw_upsert(connect, **shop)
            return True
        except:
            logger.error(traceback.format_exc())
            return False
