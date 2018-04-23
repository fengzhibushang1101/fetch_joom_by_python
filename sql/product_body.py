#coding=utf8
__author__ = 'changdongsheng'
import datetime

import sqlalchemy as SA
from sql.base import Base
from sqlalchemy import UniqueConstraint, Index
from sqlalchemy import text
from sqlalchemy.sql import and_


class ProductBody(Base):

    __tablename__ = "product_body"

    id = SA.Column(SA.INTEGER, primary_key=True, autoincrement=True)
    key = SA.Column(SA.String(128), nullable=False)  # 产品标题
    cate = SA.Column(SA.String(64), default="")
    site = SA.Column(SA.Integer(), nullable=False)  # 产品ID
    body = SA.Column(SA.Text())

    __table_args__ = (
        UniqueConstraint('key', 'site', 'cate', name='uix_key_kind_site'),  # 联合唯一索引
    )

    @classmethod
    def upsert(cls, session, **kwargs):
        pb = session.query(cls).filter(and_(cls.key == kwargs["key"], cls.site == kwargs["site"])).first() or cls()
        for k, v in kwargs.items():
            setattr(pb, k, v)
        session.add(pb)
        session.commit()
        return pb

    @staticmethod
    def raw_upsert(connect, **kwargs):
        key = kwargs["key"]
        cate = kwargs.get("cate", "") or ""
        site = kwargs.get("site", 31) or 31
        body = kwargs.get("body", "") or ""
        sql = text('insert into product_body (product_body.key, cate, site, body) values (:pb_key, :cate, :site, :body) on duplicate key update cate=:cate, body=:body')
        cursor = connect.execute(sql, pb_key=key, cate=cate, site=site, body=body)
        cursor.close()
        return True