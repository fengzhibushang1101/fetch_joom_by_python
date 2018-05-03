# coding=utf8
import traceback

__author__ = 'changdongsheng'
import datetime

import sqlalchemy as SA
from sql.base import Base
from sqlalchemy import text


class JoomShop(Base):
    __tablename__ = "joom_shop"

    id = SA.Column(SA.INTEGER, primary_key=True, autoincrement=True)
    name = SA.Column(SA.String(200), nullable=False)  # 店铺名称
    shop_no = SA.Column(SA.String(64), nullable=False, index=True, unique=True)  # 店铺ID
    logo = SA.Column(SA.String(256), nullable=False)  # 店铺logo
    rate = SA.Column(SA.FLOAT, nullable=False)  # 产品评分
    pro_count = SA.Column(SA.INTEGER, nullable=False, default=0)  # 产品总数
    reviews_count = SA.Column(SA.INTEGER, nullable=False, default=0)  # 评价总数
    r_count_30 = SA.Column(SA.INTEGER, nullable=False, default=0)  # 30天评价总数
    r_count_7 = SA.Column(SA.INTEGER, nullable=False, default=0)
    r_count_7_14 = SA.Column(SA.INTEGER, nullable=False, default=0)
    growth_rate = SA.Column(SA.FLOAT, nullable=False, default=0)
    save_count = SA.Column(SA.INTEGER, nullable=False, default=0)  # 收藏总数
    create_time = SA.Column(SA.DateTime(), nullable=False)  # 创建时间 不准确
    update_time = SA.Column(SA.DateTime(), nullable=False)  # 更新时间 不准确
    cate_id = SA.Column(SA.String(64), nullable=False, default="")  # 所属分类ID, 暂不添加
    is_verify = SA.Column(SA.String(1), nullable=False, default="0")  # 是否认证店铺

    @classmethod
    def upsert(cls, session, **kwargs):
        joom_shop = cls.find_by_no(session, kwargs["shop_no"]) or cls()
        for k, v in kwargs.iteritems():
            setattr(joom_shop, k, v)
        session.add(joom_shop)
        session.commit()
        return joom_shop

    @classmethod
    def find_by_no(cls, session, shop_no):
        return session.query(cls).filter(cls.shop_no == shop_no).first()

    @staticmethod
    def batch_upsert(connect, infos):

        try:
            new_infos = []

            for info in infos:
                name = info.get("name", "") or ""
                shop_no = info["shop_no"]
                logo = info.get("logo", "") or ""
                rate = info.get("rate", 0) or 0
                save_count = info.get("save_count", 0) or 0
                create_time = info["create_time"]
                update_time = info["update_time"]
                is_verify = info.get("is_verify", "0") or "0"
                new_infos.append(
                    dict(shop_name=name, shop_no=shop_no, logo=logo, rate=rate, save_count=save_count,
                         create_time=create_time, update_time=update_time, is_verify=is_verify))
            sql = text(
                'insert into joom_shop (joom_shop.name,shop_no,logo,rate,save_count,create_time,update_time,is_verify,pro_count,reviews_count,r_count_30,r_count_7,r_count_7_14,growth_rate,cate_id) values (:shop_name,:shop_no,:logo,:rate,:save_count,:create_time,:update_time,:is_verify,0,0,0,0,0,0,"") on duplicate key update rate=:rate, save_count=:save_count, create_time=:create_time, update_time=:update_time, is_verify=:is_verify;')
            cursor = connect.execute(sql, *new_infos)
            cursor.close()

        except Exception, e:
            print(traceback.format_exc(e))
            return False

    @staticmethod
    def raw_upsert(connect, **kwargs):
        name = kwargs.get("name", "") or ""
        shop_no = kwargs["shop_no"]
        logo = kwargs.get("logo", "") or ""
        rate = kwargs.get("rate", 0) or 0
        save_count = kwargs.get("save_count", 0) or 0
        create_time = kwargs["create_time"]
        update_time = kwargs["update_time"]
        is_verify = kwargs.get("is_verify", "0") or "0"
        sql = text(
            'insert into joom_shop (joom_shop.name,shop_no,logo,rate,save_count,create_time,update_time,is_verify,pro_count,reviews_count,r_count_30,r_count_7,r_count_7_14,growth_rate,cate_id) values (:shop_name,:shop_no,:logo,:rate,:save_count,:create_time,:update_time,:is_verify,0,0,0,0,0,0,"") on duplicate key update rate=:rate, save_count=:save_count, create_time=:create_time, update_time=:update_time, is_verify=:is_verify;')
        cursor = connect.execute(sql, shop_name=name, shop_no=shop_no, logo=logo, rate=rate, save_count=save_count,
                                 create_time=create_time, update_time=update_time, is_verify=is_verify)
        cursor.close()
        return True
