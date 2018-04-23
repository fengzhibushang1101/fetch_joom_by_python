#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
 @Time    : 2018/4/23 18:15
 @Author  : jyq
 @Software: PyCharm
 @Description: 
"""


import requests
import ujson as json

from func import random_key
from sql.session import sessionCM
from sql.category import Category


class JoomCategory(object):
    def __init__(self, auth):
        self.root_url = "https://api.joom.com/1.1/categoriesHierarchy?levels=1&language=en-US&currency=USD&_=jfrx%s"
        self.sub_url = "https://api.joom.com/1.1/categoriesHierarchy?levels=1&categoryId=%s&parentLevels=1&language=en-US&currency=USD&_=jfrx%s"
        self.headers = {"authorization": auth, "origin":"https://www.joom.com"}

    def _category(self, cid=None, parent=None):
        parent = parent or []
        if not cid:
            res = requests.get(self.root_url % random_key(4), headers=self.headers)
        else:
            res = requests.get(self.sub_url % (cid, random_key(4)), headers=self.headers)
        content = res.json()
        children = content["payload"]["children"]
        for child in children:
            item = {"id": child["id"], "name": child["name"], "has_children": child["hasPublicChildren"], "children": []}
            if item["has_children"]:
                self._category(cid=item["id"], parent=item["children"])
            parent.append(item)

    def category(self):
        root = {"id": "0", "name": "root", "has_children": True, "children": []}
        self._category(parent=root["children"])
        Category.set_status(31, "1")
        self.update_category(0, root)
        return root

    def update_category(self, p_id, cate, level=0):
        if cate["name"] == "root":
            for child in cate["children"]:
                self.update_category(0, child, level+1)
        with sessionCM() as session:
            c_id = Category.save(session, cate["id"], cate["name"], p_id, not cate["has_children"], level, 31)
            for child in cate["children"]:
                self.update_category(c_id, child, level+1)


if __name__ == "__main__":
    auth = "Bearer SEV0001MTUyMzI1NDc1MXxsU0RBOHIzTEtZOVkzQnB5UkxvbWgtRFJVWkNVRnEwY1cwLUdRWDlMc0EzamxuaU0tNVNRZ2llS1hXdUpXZXNSaENaLUZxY2RMc3hXOGRuX2pvcjMyQlh1ZXFBOWFpVTRfTHVNWUN3azJpYlktTkR2WnFfTnNRdkpUZVIwWlp3RjBtOHVfUnFsaGNhYXRTZXZOVjRqa242WUVld2hHdnRabk1rNzFlRXpzN2dDVmc9PXwci95C1oFgNvhEi5K3XOptPVwLq8u3otTCNYdGz4Dh5A=="
    jc = JoomCategory(auth)
    print jc.category()