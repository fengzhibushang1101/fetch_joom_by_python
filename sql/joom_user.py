#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
 @Time    : 2017/12/28 14:13
 @Author  : jyq
 @Software: PyCharm
 @Description: 
"""
import traceback

import sqlalchemy as SA
from lib.sql2.base import Base
from lib.utils.logger_utils import logger
from sqlalchemy import text


class JoomUser(Base):

    __tablename__ = "joom_user"

    id = SA.Column(SA.INTEGER, primary_key=True, autoincrement=True)
    user_no = SA.Column(SA.String(64), nullable=False, index=True, unique=True)
    full_name = SA.Column(SA.String(100), nullable=False)
    images = SA.Column(SA.String(256), nullable=False, default="")

    @classmethod
    def upsert(cls, session, **kwargs):
        try:
            joom_user = cls.find_by_no(session, kwargs["user_no"]) or cls(user_no=kwargs["user_no"])
            joom_user.full_name = kwargs.get("full_name", "")
            joom_user.images = kwargs.get("images", "")
            session.merge(joom_user)
            session.commit()
            return joom_user
        except:
            logger.error(traceback.format_exc())
            return None

    @classmethod
    def find_by_no(cls, session, user_no):
        return session.query(cls).filter(cls.user_no == user_no).first()

    @staticmethod
    def raw_upsert(connect, **kwargs):
        sql = text('insert into joom_user (user_no, full_name, images) values (:user_no, :full_name, :images) on duplicate key update full_name=:full_name, images = :images;')
        cursor = None
        try:
            cursor = connect.execute(sql, **kwargs)
        except:
            logger.error("joom user upsert error: %s" % sql)
        finally:
            if cursor:
                cursor.close()
        return True