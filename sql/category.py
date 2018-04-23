#coding=utf8
__author__ = 'changdongsheng'
import sqlalchemy as SA
from sql.base import Base, db
from sqlalchemy import UniqueConstraint, Index
from sqlalchemy import distinct
from sqlalchemy import text
from sqlalchemy.dialects.mysql import BIGINT


class Category(Base):
    __tablename__ = "category"

    id = SA.Column(BIGINT, primary_key=True, autoincrement=True)
    tag = SA.Column(SA.String(64), nullable=False)
    name = SA.Column(SA.String(128), nullable=False)
    p_id = SA.Column(BIGINT, nullable=False)
    is_leaf = SA.Column(SA.INTEGER, nullable=False)
    level = SA.Column(SA.INTEGER, nullable=False)
    pin = SA.Column(SA.String(8), nullable=True)
    site_id = SA.Column(BIGINT, nullable=False)
    status = SA.Column(SA.String(8), default="0")

    __table_args__ = (
        UniqueConstraint('site_id', 'tag', name="uq_site_tag"),  # 联合索引
        Index("idx_site_leaf", site_id, is_leaf),
    )

    @classmethod
    def save(cls, session, tag, name, p_id, is_leaf, level, site_id, status="0"):
        category = cls()
        category.tag = tag
        category.name = name
        category.p_id = p_id
        category.is_leaf = is_leaf
        category.level = level
        category.site_id = site_id
        category.status = status
        session.add(category)
        session.commit()
        return category.id

    @staticmethod
    def raw_save(tag, name, p_id, is_leaf, level, site_id, status="0"):
        sql = text("insert into category (tag, category.name, p_id, is_leaf, category.level, site_id, status) values (:tag, :c_name, :p_id, :is_leaf, :c_level, :site_id, :status) on duplicate key update status=:status")
        connect = db.connect()
        cursor = connect.execute(sql, tag=tag, c_name=name, p_id=p_id, is_leaf=is_leaf, c_level=level, site_id=site_id, status=status)
        cursor.close()
        s_sql = text("select id from category where tag=:tag and site_id=:site_id;")
        cursor = connect.execute(s_sql, tag=tag, site_id=site_id)
        result = cursor.fetchone()
        connect.close()
        return result[0]

    @staticmethod
    def set_status(site_id, status):
        sql = text("update category set status=:status where site_id=:site_id;")
        connect = db.connect()
        cursor = connect.execute(sql, status=status, site_id=site_id)
        cursor.close()
        connect.close()
        return True

    @classmethod
    def find_by_parent_id(cls, session, site_id, p_id):
        categories = session.query(cls).filter(
            SA.and_(
                cls.p_id == p_id,
                cls.site_id == site_id)
            ).all()
        if not categories:
            categories = list()
        return categories

    @classmethod
    def find_by_id(cls, session, category_id):
        category = session.query(cls).filter(cls.id == category_id).first()
        return category

    @classmethod
    def find_leaf_all_by_tag(cls, session, tag):
        category = session.query(cls).filter(
            cls.tag == tag,
            cls.is_leaf == 1
        ).all()
        return category

    @classmethod
    def find_by_pin(cls, session, pin):
        category = session.query(cls).filter(
            cls.pin == pin,
        ).all()
        return category

    @classmethod
    def find_by_category_id(cls, session, category_id, site_id):
        category = session.query(cls).filter(
            SA.and_(
                cls.tag == category_id,
                cls.site_id == site_id,
            )
        ).first()
        return category

    @classmethod
    def find_by_site_id(cls, session, site_id):
        categories = session.query(cls).filter(
            cls.site_id == site_id
        ).all()
        return categories

    @classmethod
    def return_id_by_p_tag(cls, session, site_id, p_tag):
        print p_tag
        category = session.query(cls).filter(
            SA.and_(
                cls.site_id == site_id,
                cls.tag == p_tag
            )
        ).one()
        return category


    @classmethod
    def find_by_around_id(cls, session, start, stop):
        cs = session.query(cls).filter(
            SA.and_(
                cls.id >= start,
                cls.id < stop
            )
        ).all()
        if not cs:
            return list()
        else:
            return cs

    @classmethod
    def find_leaf_by_site_id(cls, session, site_id):
        cs = session.query(cls).filter(
            SA.and_(
                cls.site_id == site_id,
                cls.is_leaf == 1
            )
        ).all()
        if not cs:
            return list()
        else:
            return cs

    @classmethod
    def get_traversed_site(cls, session, start_id):
        sites = session.query(distinct(cls.site_id)).filter(
            cls.id < start_id
        ).all()
        if not sites:
            return list()
        ll = list()
        for m in sites:
            for n in m:
                ll.append(n)
        return ll

    @classmethod
    def amazon_update_p_id(cls, session, site_id, id, parent_id):
        category = session.query(cls).filter(
            SA.and_(
                cls.site_id == site_id,
                cls.tag == id
            )
        ).one()
        category.p_id = parent_id
        session.add(category)
        session.commit()
        return id

    @classmethod
    def update02(cls, session, shop_id, id, is_leaf):
        category = session.query(cls).filter(
            SA.and_(
                cls.site_id == shop_id,
                cls.id == id
            )
        ).one()
        category.tag = is_leaf
        session.add(category)
        session.commit()
        return id

    @classmethod
    def amazon_update_tag(cls, session, site_id, id, parent_id):
        category = session.query(cls).filter(
            SA.and_(
                cls.site_id == site_id,
                cls.tag == id
            )
        ).one()
        category.tag = parent_id
        session.add(category)
        session.commit()
        return id

    @classmethod
    def amazon_update_tag_and_p(cls, session, site_id, id, parent_id, tag):
        category = session.query(cls).filter(
            SA.and_(
                cls.site_id == site_id,
                cls.id == id
            )
        ).one()
        category.tag = tag
        category.p_id = parent_id
        session.add(category)
        session.commit()
        return id

    @classmethod
    def find_all(cls, session, shop_id):
        return session.query(cls).filter(cls.site_id == shop_id).all()

    @classmethod
    def find_child(cls, session, shop_id, parent_id):
        categories = session.query(cls).filter(
            SA.and_(
                cls.site_id == shop_id,
                cls.p_id == parent_id
            )
        ).first()
        return categories

    @classmethod
    def find_by_site_tag(cls, session, site_id, tag):
        category = session.query(cls).filter(
            SA.and_(
                cls.site_id == site_id,
                cls.tag == tag
            )
        ).first()
        return category

    @classmethod
    def find_by_site_tag_all(cls, session, site_id, tag):
        category = session.query(cls).filter(
            SA.and_(
                cls.site_id == site_id,
                cls.tag == str(tag)
            )
        ).all()
        return category

    @classmethod
    def find_by_site_and_leaf_name(cls, session, site_id, name):
        category = session.query(cls).filter(
            SA.and_(
                cls.site_id == site_id,
                cls.name == name,
                cls.is_leaf == 1
            )
        ).first()
        return category

    @classmethod
    def find_by_leaf_name(cls, session, site_id, name):
        category = session.query(cls).filter(
            SA.and_(
                cls.site_id == site_id,
                cls.name == name,
                cls.is_leaf == 1
            )
        ).all()
        return category

    def to_json(self):
        return {
            "id": self.id,
            "tag": self.tag,
            "name": self.name,
            "p_id": self.p_id,
            "is_leaf": self.is_leaf,
            "level": self.level,
            "pin": self.pin,
            "site_id": self.site_id,
            "status": self.status
        }

    @classmethod
    def to_object(cls, category):
        return cls(
            id=category["id"],
            tag=category["tag"],
            name=category["name"],
            p_id=category["p_id"],
            is_leaf=category["is_leaf"],
            level=category["level"],
            pin=category["pin"],
            site_id=category["site_id"],
            status=category["status"]
        )

    @classmethod
    def save_amazon_category(cls, session, tag, name, p_id, is_leaf, level, site_id, pin):
        category = cls()
        category.tag = tag
        category.name = name
        category.p_id = p_id
        category.is_leaf = is_leaf
        category.level = level
        category.site_id = site_id
        category.pin = pin
        session.add(category)
        session.commit()
        return category.id

    @classmethod
    def amazon_update_by_singal_tag(cls, session, site_id, tag, feed):
        category = session.query(cls).filter(
            SA.and_(
                cls.site_id == site_id,
                cls.tag == tag
            )
        ).one()
        category.name = feed["name"]
        category.p_id = feed["parent_id"]
        category.tag = feed["key"]
        category.is_leaf = feed["is_leaf"]
        category.level = feed["level"]
        category.pin = feed["pin"]
        session.add(category)
        session.commit()
        return category.id

    @classmethod
    def amazon_update_name_by_id(cls, session, id, name):
        category = session.query(cls).filter(
            SA.and_(
                cls.id == id,
            )
        ).one()
        category.name = name
        session.add(category)
        session.commit()
        return category.id

    @classmethod
    def amazon_update_percisely(cls, session, cur, p_tag, feed):
        category = session.query(cls).filter(
            SA.and_(
                cls.site_id == cur.site_id,
                cls.tag == cur.tag,
                cls.p_id == cur.p_id,
                cls.id == cur.id,
            )
        ).one()
        # print s.id, s.tag, s.name, s.p_id, s.is_leaf, s.level, s.site_id, s.pin
        category.name = feed["name"]
        category.p_id = p_tag
        category.tag = feed["key"]
        category.is_leaf = feed["is_leaf"]
        category.level = feed["level"]
        category.pin = feed["pin"]
        session.add(category)
        session.commit()
        return category.id

    @classmethod
    def find_amazon_by_parent_id(cls, session, site_id, p_id):
        categories = session.query(cls).filter(
            SA.and_(
                cls.id == p_id,
                cls.site_id == site_id)
            ).one()
        if not categories:
            categories = None
        return categories

    @classmethod
    def find_amazon_by_p_id(cls, session, site_id, p_id):
        categories = session.query(cls).filter(
            SA.and_(
                cls.p_id == p_id,
                cls.site_id == site_id)
        ).all()
        if not categories:
            categories = None
        return categories

    @classmethod
    def check_dup(cls, session, site_id, tag):
        category = session.query(cls).filter(
            SA.and_(
                cls.site_id == site_id,
                cls.tag == tag
            )
        ).first()
        return category

    @classmethod
    def find_by_regex(cls, session, site_id, regex):
        """用于亚马逊查看是否有未执行完的category更新"""
        category = session.query(cls).filter(
            SA.and_(
                cls.site_id == site_id,
                cls.tag.op('regexp')(regex),
            )
        ).all()
        return category

    @classmethod
    def find_by_name_like(cls, session, site_id, like):
        """用于亚马逊查看是否有未执行完的category更新"""
        category = session.query(cls).filter(
            SA.and_(
                cls.site_id == site_id,
                cls.name.like("%" + like + "%"),
            )
        ).all()
        return category
