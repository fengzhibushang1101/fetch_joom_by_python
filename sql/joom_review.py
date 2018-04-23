#coding=utf8
__author__ = 'changdongsheng'
import traceback

import sqlalchemy as SA
from lib.sql2.base import Base
from lib.utils.logger_utils import logger
from sqlalchemy import text

class JoomReview(Base):

    __tablename__ = "joom_review"

    id = SA.Column(SA.INTEGER, primary_key=True, autoincrement=True)
    review_no = SA.Column(SA.String(64), nullable=False, index=True, unique=True)  # 评论ID
    pro_no = SA.Column(SA.String(64), nullable=False)  # 产品ID
    user_no = SA.Column(SA.String(64), nullable=False)  # 评论 user_no
    language = SA.Column(SA.String(10), nullable=False)  # 评论原始语言
    origin_text = SA.Column(SA.Text(), nullable=False, default="")  # 评论原始语言text
    new_text = SA.Column(SA.Text(), nullable=False, default="")  # 应该是英文语言text
    photos = SA.Column(SA.String(5120), nullable=False, default="")  # 图片信息
    order_id = SA.Column(SA.String(64), nullable=False, default="")  # 订单ID 目前Joom没有实装
    is_anonymous = SA.Column(SA.String(16), nullable=False, default="0")  # 评论是否匿名
    variation_id = SA.Column(SA.String(64), nullable=False, default="")  # 变体ID
    colors = SA.Column(SA.String(512), nullable=False, default="")  # 变体颜色信息
    star = SA.Column(SA.INTEGER, nullable=False)  # 评分
    create_time = SA.Column(SA.DateTime(), nullable=False)  # 评论创建时间
    update_time = SA.Column(SA.DateTime(), nullable=False)  # 评论修改时间
    shop_no = SA.Column(SA.String(64), nullable=False)  # 店铺ID

    @classmethod
    def upsert(cls, session, **kwargs):
        joom_review = cls.find_by_no(session, kwargs["review_no"]) or cls(review_no=kwargs["review_no"])
        del kwargs["review_no"]
        for k, v in kwargs.items():
            if isinstance(v, unicode):
                v = v.encode("utf8")
            if not isinstance(v, basestring):
                v = str(v)
            setattr(joom_review, k, v)
        session.add(joom_review)
        session.commit()
        return joom_review

    @classmethod
    def find_by_no(cls, session, review_no):
        return session.query(cls).filter(cls.review_no == review_no).first()

    @staticmethod
    def raw_upsert(connect, **kwargs):
        try:
            review_no = kwargs["review_no"]
            create_time = kwargs["create_time"]
            update_time = kwargs["update_time"]
            pro_no = kwargs["pro_no"]
            variation_id = kwargs["variation_id"] or ""
            user_no = kwargs["user_no"]
            language = kwargs["language"] or "en"
            origin_text = kwargs["origin_text"] or ""
            new_text = kwargs["new_text"] or ""
            order_id = kwargs["order_id"]
            is_anonymous = kwargs["is_anonymous"] or "0"
            colors = kwargs["colors"]
            star = kwargs["star"]
            shop_no = kwargs["shop_no"]
            photos = kwargs.get("photos", "") or ""
            sql = text('insert into joom_review (review_no,create_time,update_time,pro_no,variation_id,user_no,joom_review.language,origin_text,new_text,order_id,is_anonymous,colors,star,shop_no,photos) VALUES (:review_no,:create_time,:update_time,:pro_no,:variation_id,:user_no,:rev_language,:origin_text,:new_text,:order_id,:is_anonymous,:colors,:star,:shop_no,:photos) on duplicate key update star=:star;')
            cursor = connect.execute(sql, review_no=review_no, create_time=create_time, update_time=update_time, pro_no=pro_no, variation_id=variation_id, user_no=user_no, rev_language=language, origin_text=origin_text, new_text=new_text, order_id=order_id, is_anonymous=is_anonymous, colors=colors, star=star, shop_no=shop_no, photos=photos)
            cursor.close()
            return True
        except:
            print traceback.format_exc()
            return False