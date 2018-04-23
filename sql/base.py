#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
 @Time    : 2018/4/2 19:53
 @Author  : jyq
 @Software: PyCharm
 @Description: 
"""
import contextlib

import sqlalchemy as SA
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

mysql_db = {
    "user": "myweb",
    "password": "MyNewPass4!",
    "host": "127.0.0.1",
    "db": "andata",
}
db = SA.create_engine(
    "mysql://%s:%s@%s/%s?charset=utf8mb4" % (mysql_db["user"], mysql_db["password"], mysql_db["host"], mysql_db["db"]),
    echo=False,
    pool_recycle=3600,
    pool_size=5000
)


Base = declarative_base()
metadata = Base.metadata


session_maker = sessionmaker(bind=db)

def get_session():
    """
    链接到数据库的SESSION
    """
    return session_maker()


@contextlib.contextmanager
def sessionCM():
    session = get_session()
    try:
        yield session
    except Exception, e:
        raise
    finally:
        session.close()