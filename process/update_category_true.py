#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
 @Time    : 2018/4/27 10:25
 @Author  : jyq
 @Software: PyCharm
 @Description: 
"""
import requests

from func import random_key, get_joom_token
from sql.base import sessionCM
from sql.category import Category


class JoomCategory(object):
    def __init__(self, auth):
        self.root_url = "https://api.joom.com/1.1/categoriesHierarchy?levels=1&language=en-US&currency=USD&_=jfrx%s"
        self.sub_url = "https://api.joom.com/1.1/categoriesHierarchy?levels=1&categoryId=%s&parentLevels=1&language=en-US&currency=USD&_=jfrx%s"
        self.headers = {"authorization": auth, "origin": "https://www.joom.com"}

    def category_stalker(self, p_tag=None, level=1, p_id=0):
        print u"正在采集%s的记录" % p_tag
        if not p_tag:
            res = requests.get(self.root_url % random_key(4), headers=self.headers, timeout=10)
        else:
            res = requests.get(self.sub_url % (p_tag, random_key(4)), headers=self.headers, timeout=10)
        if "unauthorized" in res.content:
            auth = get_joom_token()
            self.headers = {"authorization": auth, "origin": "https://www.joom.com"}
            self.category_stalker(p_tag, level, p_id)
        n_level = level + 1
        content = res.json()
        c_infos = content["payload"]["children"]
        with sessionCM() as session:
            for c_info in c_infos:
                print c_info
                tag = c_info['id']
                name = c_info['name']
                is_leaf = 0 if c_info["hasPublicChildren"] else 1
                cate = Category.find_by_site_tag(session, 31, tag)
                if not cate:
                    n_p_id = Category.save(session, tag, name, p_id, is_leaf, level, 31)
                else:
                    n_p_id = cate.id
                if not is_leaf:
                    self.category_stalker(p_tag=tag, level=n_level, p_id=n_p_id)

    def begin_stalk(self):
        self.category_stalker()
