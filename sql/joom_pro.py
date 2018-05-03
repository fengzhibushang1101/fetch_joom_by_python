# coding=utf8
__author__ = 'changdongsheng'
import datetime
import traceback

import sqlalchemy as SA
from sql.base import Base, db
from sql.session import sessionCM
# from lib.utils.logger_utils import logger
from sqlalchemy import text


class JoomPro(Base):
    __tablename__ = "joom_pro"

    id = SA.Column(SA.Integer(), primary_key=True, autoincrement=True)
    name = SA.Column(SA.String(512), nullable=False)  # 产品标题
    pro_no = SA.Column(SA.String(64), nullable=False, index=True, unique=True)  # 产品ID
    image = SA.Column(SA.String(256), nullable=False)  # 产品主图
    category_id = SA.Column(SA.String(64), nullable=False)  # 产品叶子分类ID
    cate_id1 = SA.Column(SA.String(64), nullable=False, default="")  # 产品一级分类ID
    cate_id2 = SA.Column(SA.String(64), nullable=False, default="")  # 产品二级分类ID
    cate_id3 = SA.Column(SA.String(64), nullable=False, default="")
    cate_id4 = SA.Column(SA.String(64), nullable=False, default="")
    cate_id5 = SA.Column(SA.String(64), nullable=False, default="")
    rate = SA.Column(SA.Float(), nullable=False)  # 产品评分
    msrp = SA.Column(SA.Float(), nullable=False, default=0)  # MSRP
    origin_price = SA.Column(SA.Float(), nullable=False, default=0)  # 卖家设置价格 目前没用到
    discount = SA.Column(SA.Integer(), nullable=False, default=0)  # 产品打折比例
    real_price = SA.Column(SA.Float(), nullable=False)  # 产品实际价格
    reviews_count = SA.Column(SA.Integer(), nullable=False, default=0)  # 产品评价总数
    r_count_30 = SA.Column(SA.Integer(), nullable=False, default=0)  # 产品30天评价数
    r_count_7 = SA.Column(SA.Integer(), nullable=False, default=0)  # 产品7天内评价数
    r_count_7_14 = SA.Column(SA.Integer(), nullable=False, default=0)  # 产品7-14天评价数
    growth_rate = SA.Column(SA.Float(), nullable=False, default=0)  # 产品评价上升率  ((r_count_7/r_count_7_14)-1) * 100
    save_count = SA.Column(SA.Integer(), nullable=False, default=0)  # 收藏总数 目前没用到
    shop_no = SA.Column(SA.String(64), nullable=False)
    create_time = SA.Column(SA.DateTime(), nullable=False)  # 产品创建时间 根据最早变体创建时间计算
    update_time = SA.Column(SA.DateTime(), nullable=False)  # 产品修改时间 根据最近变体修改时间计算

    @classmethod
    def upsert(cls, session, **kwargs):
        joom_pro = cls.find_by_no(session, kwargs["pro_no"]) or cls()
        for k, v in kwargs.iteritems():
            setattr(joom_pro, k, v)
        session.add(joom_pro)
        session.commit()
        return joom_pro

    @classmethod
    def find_by_no(cls, session, pro_no):
        return session.query(cls).filter(cls.pro_no == pro_no).first()

    @classmethod
    def update_review_info(cls, session, pro_no, rc30, rc7, rc714, growth_rate):
        joom_pro = cls.find_by_no(session, pro_no=pro_no)
        joom_pro.r_count_30 = rc30
        joom_pro.r_count_7 = rc7
        joom_pro.r_count_7_14 = rc714
        joom_pro.growth_rate = growth_rate
        session.add(joom_pro)
        session.commit()
        return joom_pro

    @classmethod
    def update(cls, session, **kwargs):
        joom_pro = cls.find_by_no(session, pro_no=kwargs["pro_no"])
        for k, v in kwargs.items():
            setattr(joom_pro, k, v)
        session.add(joom_pro)
        session.commit()
        return joom_pro

    @staticmethod
    def raw_update(connect, **kwargs):
        sql = text(
            'update joom_pro set r_count_30=:r_count_30, r_count_7=:r_count_7, r_count_7_14=:r_count_7_14, growth_rate=:growth_rate where pro_no=:pro_no;')
        cursor = connect.execute(sql, **kwargs)
        cursor.close()
        return True

    @staticmethod
    def batch_upsert(connect, infos):

        try:
            new_infos = []
            for info in infos:
                name = info.get("name", "")
                pro_no = info["pro_no"]
                shop_no = info["shop_no"]
                category_id = info.get("category_id", "")
                image = info.get("image", "")
                rate = info.get("rate", 0)
                msrp = info.get("msrp", 0)
                discount = info.get("discount", 0)
                real_price = info.get("real_price", 0)
                reviews_count = info.get("reviews_count", 0)
                create_time = info["create_time"]
                update_time = info["update_time"]
                # new_infos.append((name, pro_no, shop_no, category_id, image, rate, msrp, discount, real_price,
                #                   reviews_count, create_time, update_time, name, category_id, rate, msrp, discount, real_price, reviews_count, update_time))
                new_infos.append(dict(jp_name=name, pro_no=pro_no, shop_no=shop_no, category_id=category_id, image=image, rate=rate,
                     msrp=msrp, discount=discount, real_price=real_price, reviews_count=reviews_count,
                     create_time=create_time, update_time=update_time))
            sql =text('insert into joom_pro (joom_pro.name,pro_no,shop_no,category_id,image,rate,msrp,discount,real_price,reviews_count,create_time,update_time,cate_id1,cate_id2,cate_id3,cate_id4,cate_id5,origin_price,r_count_30,r_count_7,r_count_7_14,growth_rate,save_count) values (:jp_name,:pro_no,:shop_no,:category_id,:image,:rate,:msrp,:discount,:real_price,:reviews_count,:create_time,:update_time,"","","","","",0,0,0,0,0,0) on duplicate key update joom_pro.name=VALUES(joom_pro.name),category_id=VALUES(category_id),rate=VALUES(rate),msrp=VALUES(msrp),discount=VALUES(discount),real_price=VALUES(real_price),reviews_count=VALUES(reviews_count),update_time=VALUES(update_time);')
            # sql = text('insert into joom_pro (joom_pro.name,pro_no,shop_no,category_id,image,rate,msrp,discount,real_price,reviews_count,create_time,update_time,cate_id1,cate_id2,cate_id3,cate_id4,cate_id5,origin_price,r_count_30,r_count_7,r_count_7_14,growth_rate,save_count) values (?, ?, ?,?,?,?,?,?,?,?,?,?,"","","","","",0,0,0,0,0,0) on duplicate key update joom_pro.name=?,category_id=?,rate=?,msrp=?,discount=?,real_price=?,reviews_count=?,update_time=?;')
            # print type(new_infos[0])
            cursor = connect.execute(sql, *new_infos)
            cursor.close()
        except Exception, e:
            print(traceback.format_exc(e))
            return False

    @staticmethod
    def raw_upsert(connect, **kwargs):
        try:
            name = kwargs.get("name", "") or ""
            pro_no = kwargs["pro_no"]
            shop_no = kwargs["shop_no"]
            category_id = kwargs.get("category_id", "") or ""
            image = kwargs.get("image", "") or ""
            rate = kwargs.get("rate", 0) or 0
            msrp = kwargs.get("msrp", 0) or 0
            discount = kwargs.get("discount", 0) or 0
            real_price = kwargs.get("real_price", 0) or 0
            reviews_count = kwargs.get("reviews_count", 0) or 0
            create_time = kwargs["create_time"]
            update_time = kwargs["update_time"]
            sql = text(
                'insert into joom_pro (joom_pro.name,pro_no,shop_no,category_id,image,rate,msrp,discount,real_price,reviews_count,create_time,update_time,cate_id1,cate_id2,cate_id3,cate_id4,cate_id5,origin_price,r_count_30,r_count_7,r_count_7_14,growth_rate,save_count) values (:jp_name,:pro_no,:shop_no,:category_id,:image,:rate,:msrp,:discount,:real_price,:reviews_count,:create_time,:update_time,"","","","","",0,0,0,0,0,0) on duplicate key update joom_pro.name=:jp_name,category_id=:category_id,rate=:rate,msrp=:msrp,discount=:discount,real_price=:real_price,reviews_count=:reviews_count,update_time=:update_time;')
            cursor = connect.execute(sql, jp_name=name, pro_no=pro_no, shop_no=shop_no, category_id=category_id,
                                     image=image, rate=rate, msrp=msrp, discount=discount, real_price=real_price,
                                     reviews_count=reviews_count, create_time=create_time, update_time=update_time)
            cursor.close()
            return True
        except:
            print(traceback.format_exc())
            return False

    @classmethod
    def pro_review_cnt(cls, pro_no):
        try:
            connect = db.connect()
            sql = text("select count(*) from joom_review where pro_no=:pro_no;")
            cursor = connect.execute(sql, pro_no=pro_no)
            return cursor.fetchone()[0]
        except:
            return 0
        finally:
            cursor.close()
            connect.close()

if __name__ == "__main__":
    from sql.base import db
    connect = db.connect()
    JoomPro.batch_upsert(connect, [{
        "pro_no": "111111111",
        "shop_no": "222222222",
        "create_time": datetime.datetime.now(),
        "update_time": datetime.datetime.now(),
    }, {
        "pro_no": "3333333333123",
        "shop_no": "222222123222",
        "create_time": datetime.datetime.now(),
        "update_time": datetime.datetime.now(),
    }
    ])
    connect.execute(
        text("INSERT INTO test (name, value) VALUES (:name, :value)"),
        *[{'name': 1, 'value': 2},
        {'name': 3, 'value': 4}]
    )
