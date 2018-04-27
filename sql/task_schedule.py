# coding=utf8
__author__ = 'changdongsheng'
import time
import traceback

import sqlalchemy as SA
from sql.base import Base, db, sessionCM
from sqlalchemy import UniqueConstraint, Index, text


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
        session.execute(cls.__table__.insert(), infos)
        session.commit()

    @classmethod
    def find_by_kind_status_limit(cls, session, kind, status="0", site=31, limit=10000, offset=0):
        return session.query(cls.key).filter(SA.and_(
            cls.site == site,
            cls.kind == kind,
            cls.status == status,
        )).offset(offset).limit(limit)

    @classmethod
    def get_init_raw(cls, kind, site=31, limit=10000, offset=0):
        with sessionCM() as session:
            infos = session.query(cls).filter(SA.and_(
                cls.site == site,
                cls.kind == kind,
                cls.status == cls.INIT,
            )).offset(offset).limit(limit)
            r_infos = list()
            for info in infos:
                info.status = cls.DOING
                session.add(info)
                r_infos.append({
                    "next_token": info.next_token,
                    "key": info.key,
                    "dealtime": info.dealtime,
                    "error_times": info.error_times
                })
            session.commit()
            return r_infos

    @classmethod
    def clear(cls):
        connect = db.connect()
        create_str = cls.get_create_table_str()
        connect.execute('drop table %s;' % cls.__tablename__)
        connect.execute(create_str)
        connect.close()

    @classmethod
    def get_create_table_str(cls):
        connect = db.connect()
        res = connect.execute('show create table %s' % cls.__tablename__)
        connect.close()
        return res.first()[1]

    @staticmethod
    def raw_set(site, kind, key, status, dealtime, error_times=0, next_token=None):
        sql = text(
            'update task_schedule set status=:status where task_schedule.key=:ts_key and kind=:kind and site=:site and error_time=:error_times and dealtime=:dealtime and next_token=:next_token;')
        connect = db.connect()
        cursor = connect.execute(sql, status=status, ts_key=key, kind=kind, site=site, dealtime=dealtime,
                                 error_times=error_times, next_token=next_token)
        cursor.close()
        connect.close()
        return True

    @classmethod
    def raw_pure_upsert(cls, key_lst, kind, site):
        connect = db.connect()
        if isinstance(key_lst[0], list):
            for k_lst in key_lst:
                values = zip(k_lst, [kind] * len(k_lst), [site] * len(k_lst), [TaskSchedule.INIT] * len(k_lst),
                             [0] * len(k_lst))
                cls._raw_batch_upsert(connect, values)
        else:
            values = zip(key_lst, [kind] * len(key_lst), [site] * len(key_lst), [TaskSchedule.INIT] * len(key_lst),
                         [0] * len(key_lst))
            cls._raw_batch_upsert(connect, values)
        connect.close()
        return True

    @classmethod
    def _raw_batch_upsert(cls, connect, values):
        values_map = ['("%s", "%s", %s, %s, %s)' % item for item in values]
        sql = 'insert into task_schedule (task_schedule.key, kind, site, status, dealtime) values %s;' % ",".join(
            values_map)
        sql = text(sql)
        cursor = connect.execute(sql)
        cursor.close()
        return True


if __name__ == "__main__":
    print TaskSchedule.__table__.insert
