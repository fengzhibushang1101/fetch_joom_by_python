# coding=utf8
__author__ = 'changdongsheng'
import time
import traceback

import sqlalchemy as SA
from sql.base import Base, db
from sqlalchemy import UniqueConstraint, Index


class TaskSchedule(Base):
    __tablename__ = "task_schedule"

    INIT, DOING, DONE, PEND, ERROR = 0, 1, 2, 3, 4
    STEP = 5
    id = SA.Column(SA.INTEGER, primary_key=True, autoincrement=True)
    key = SA.Column(SA.String(128), nullable=False)
    site = SA.Column(SA.Integer(), nullable=False)  # 31
    kind = SA.Column(SA.String(16), nullable=False)  # 分类：目录，产品，评论
    status = SA.Column(SA.Integer(), default=INIT)  # 0-初始态；1-执行态；2-结束态; 4-错误
    next_token = SA.Column(SA.TEXT)
    dealtime = SA.Column(SA.Integer(), default=0)
    error_times = SA.Column(SA.Integer(), nullable=False, default=0)

    __table_args__ = (
        UniqueConstraint('key', 'kind', 'site', name="uq_idx_key_kind_site"),  # 联合索引
        Index('ix_site_kind_status', 'site', 'kind', 'status'),  # 联合索引
    )

    @classmethod
    def insert(cls, session, **kwargs):
        info = cls()
        for k, v in kwargs.iteritems():
            setattr(info, k, v)
        session.add(info)
        session.commit()

    @classmethod
    def batch_insert(cls, session, kind, keys, site=31):
        infos = map(lambda x: {"key": x, "site": site, "kind": kind}, keys)
        session.excuce(cls.__table__.Insert(), infos)
        session.add()

    @classmethod
    def find_by_kind_status_limit(cls, session, kind, status="0", site=31, limit=10000, offset=0):
        return session.query(cls.key).filter(SA.and_(
            cls.site == site,
            cls.kind == kind,
            cls.status == status,
        )).offset(offset).limit(limit)

    @classmethod
    def clear(cls):
        connect = db.connect()
        create_str = cls.get_create_table_str
        connect.execute('drop table %s; %s;' % (cls.__tablename__, create_str))
        connect.close()

    @classmethod
    def get_create_table_str(cls):
        connect = db.connect()
        res = connect.execute('show create table %s' % cls.__tablename__)
        connect.close()
        return res.first()[1]


if __name__ == "__main__":
    print TaskSchedule.get_create_table_str()
