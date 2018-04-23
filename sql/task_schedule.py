#coding=utf8
__author__ = 'changdongsheng'
import time
import traceback

import sqlalchemy as SA
from sql.base import Base, db
from sql.session import sessionCM
from sqlalchemy import UniqueConstraint, Index
from sqlalchemy import text
from sqlalchemy.sql import and_, or_


class TaskSchedule(Base):

    __tablename__ = "task_schedule"

    INIT, DOING, DONE, PEND = 0, 1, 2, 3
    STEP = 5
    id = SA.Column(SA.INTEGER, primary_key=True, autoincrement=True)
    key = SA.Column(SA.String(64), nullable=False)
    value = SA.Column(SA.String(256))
    site = SA.Column(SA.Integer(), nullable=False)
    kind = SA.Column(SA.String(16), nullable=False) #分类：目录，产品，评论
    status = SA.Column(SA.Integer(), default=INIT) #0-初始态；1-执行态；2-结束态
    dealtime = SA.Column(SA.Integer(), default=0)
    body = SA.Column(SA.Text())

    __table_args__ = (
        UniqueConstraint('key', 'kind', 'site', name="uq_idx_key_kind_site"),  # 联合索引
        Index('ix_site_kind_status', 'site', 'kind', 'status'),  # 联合索引
    )

    @classmethod
    def upsert(cls, session, **kwargs):
        try:
            ts = cls.find_by_no(session, kwargs["key"], kwargs["kind"], kwargs["site"]) or cls()
            for k, v in kwargs.items():
                setattr(ts, k, v)
            session.merge(ts)
            session.commit()
            return ts
        except:
            # logger.error(traceback.format_exc())
            return None

    @classmethod
    def batch_upsert(cls, session, **kwargs):
        try:
            for k_lst in kwargs["keys"]:
                if isinstance(k_lst, list):
                    for k in k_lst:
                        ts = cls.find_by_no(session, k, kwargs["kind"], kwargs["site"]) or cls()
                        ts.key = k
                        for _k, _v in kwargs.items():
                            if _k != "keys":
                                setattr(ts, _k, _v)
                        session.add(ts)
                    session.commit()
                else:
                    ts = cls.find_by_no(session, k_lst, kwargs["kind"], kwargs["site"]) or cls()
                    ts.key = k_lst
                    for _k, _v in kwargs.items():
                        if _k != "keys":
                            setattr(ts, _k, _v)
                    session.add(ts)
                    session.commit()
            return True
        except:
            # logger.error(traceback.format_exc())
            return None

    @classmethod
    def find_by_no(cls, session, key, kind, site):
        return session.query(cls).filter(and_(cls.key == key, cls.kind == kind, cls.site == site)).first()

    @classmethod
    def get(cls, kind, site):
        with sessionCM() as session:
            ts = session.query(cls).filter(and_(cls.site == site, cls.kind == kind, cls.status == cls.INIT)).first()
            if ts:
                ts.status = cls.DOING
                ts.dealtime = int(time.time())
                session.merge(ts)
                session.commit()
                return ts.to_dict()
            else:
                ts = session.query(cls).filter(and_(cls.site == site, cls.kind == kind, cls.status == cls.DOING)).first()
                if ts:
                    ts.dealtime = int(time.time())
                    session.merge(ts)
                    session.commit()
                    return ts.to_dict()
                else:
                    return "ok"

    @classmethod
    def get_kind_batch(cls, site, kind):
        with sessionCM() as session:
            ts_lst = session.query(cls).filter(and_(cls.site == site, cls.kind == kind, cls.status == cls.INIT)).limit(5000).all()
            ts_lst = [ts for ts in ts_lst]
            if not ts_lst:
                now = int(time.time())
                ts_lst = session.query(cls).filter(and_(cls.site == site, cls.kind == kind, cls.status == cls.DOING)).filter(cls.dealtime < now).limit(5000).all()
            for ts in ts_lst:
                ts.status = cls.DOING
                ts.dealtime = int(time.time()) + cls.STEP
                session.add(ts)
            session.commit()
            ts_lst = [ts.to_dict() for ts in ts_lst]
            return ts_lst

    @classmethod
    def get_cate(cls, site):
        with sessionCM() as session:
            ts = session.query(cls).filter(and_(cls.site == site, cls.kind == "cate", cls.status == cls.INIT)).first()
            if ts:
                return ts.to_dict()
            return None

    @classmethod
    def set(cls, site, kind, key, status):
        with sessionCM() as session:
            ts = session.query(cls).filter(and_(cls.key == key, cls.kind == kind, cls.site == site)).first()
            if ts:
                ts.status = status
                session.add(ts)
                session.commit()
                return ts.to_dict()
            return None

    @classmethod
    def update(cls, site, kind, key, value=None, status=0):
        with sessionCM() as session:
            task = session.query(cls).filter(and_(cls.key == key, cls.kind == kind, cls.site == site)).first()
            if task:
                # logger.debug("update params: key: %s, value: %s, status: %s" % (key, value, status))
                task.status = status
                task.value = value
                session.add(task)
                session.commit()
                return task.to_dict()
            return None

    @classmethod
    def is_complete(cls, kind):
        with sessionCM() as session:
            ts = session.query(cls).filter(cls.kind == kind).filter(or_(cls.status == cls.DOING, cls.status == cls.INIT)).first()
            if not ts:
                return True
            return False

    @classmethod
    def is_starting(cls, kind):
        with sessionCM() as session:
            ts = session.query(cls).filter(cls.kind == kind).filter(or_(cls.status == cls.DOING, cls.status == cls.DONE, cls.status == cls.PEND)).first()
            if not ts:
                return False
            return True

    def to_dict(self):
        return {
            "id": self.id,
            "key": self.key,
            "value": self.value,
            "kind": self.kind,
            "site": self.site,
            "status": self.status,
            "dealtime": self.dealtime,
            "body": self.body
        }

    @staticmethod
    def get_raw_kind_batch(site, kind):
        try:
            now = int(time.time())
            connect = db.connect()
            sql = text('select task_schedule.key, task_schedule.value from task_schedule where site=:site and kind=:kind and status in (0,1) and dealtime < :now limit 10000;')
            cursor = connect.execute(sql, site=site, kind=kind, now=now)
            result = []
            dealtime = int(time.time()) + TaskSchedule.STEP
            for item in cursor:
                sql = text('update task_schedule set status=1, dealtime=:dealtime where task_schedule.key=:ts_key and kind=:kind and site=:site;')
                u_cursor = connect.execute(sql, dealtime=dealtime, ts_key=item[0], kind=kind, site=site)
                u_cursor.close()
                result.append({"key": item[0].encode("utf8") if isinstance(item[0], unicode) else item[0], "value": item[1].encode("utf8") if (item[1] and isinstance(item[1], unicode)) else (item[1] or "")})
            cursor.close()
            connect.close()
            return result
        except:
            # logger.error(traceback.format_exc())
            return []

    @staticmethod
    def is_raw_complete(kind):
        connect = db.connect()
        sql = text('select task_schedule.key from task_schedule where kind=:kind and status in (0,1) limit 1;')
        cursor = connect.execute(sql, kind=kind)
        result = cursor.fetchone()
        cursor.close()
        connect.close()
        if result:
            return False
        return True

    @staticmethod
    def is_raw_starting(kind):
        connect = db.connect()
        sql = text('select task_schedule.key from task_schedule where kind=:kind and status in (1,2);')
        cursor = connect.execute(sql, kind=kind)
        ts = cursor.fetchone()
        cursor.close()
        connect.close()
        if ts:
            return True
        return False

    @staticmethod
    def raw_update(site, kind, key, value="", status=0):
        try:
            sql = text('update task_schedule set task_schedule.value=:ts_value, status=:status where task_schedule.key=:ts_key and kind=:kind and site=:site;')
            connect = db.connect()
            cursor = connect.execute(sql, ts_value=value, status=status, ts_key=key, kind=kind, site=site)
            cursor.close()
            connect.close()
            return True
        except:
            # logger.error(traceback.format_exc())
            # logger.error("key: %s, value: %s" % (key, value))
            return False

    @staticmethod
    def raw_set(site, kind, key, status):
        # sql = 'update task_schedule set status=%s where task_schedule.key="%s" and kind="%s" and site=%s;' % (status, key, kind, site)
        sql = text('update task_schedule set status=:status where task_schedule.key=:ts_key and kind=:kind and site=:site;')
        connect = db.connect()
        cursor = connect.execute(sql, status=status, ts_key=key, kind=kind, site=site)
        cursor.close()
        connect.close()
        return True

    @staticmethod
    def raw_pure_upsert(key_lst, kind, site):
        connect = db.connect()
        if isinstance(key_lst[0], list):
            for k_lst in key_lst:
                values = zip(k_lst, [kind]*len(k_lst), [site]*len(k_lst), [TaskSchedule.INIT]*len(k_lst), [0]*len(k_lst))
                TaskSchedule._raw_batch_upsert(connect, values)
        else:
            values = zip(key_lst, [kind]*len(key_lst), [site]*len(key_lst), [TaskSchedule.INIT]*len(key_lst), [0]*len(key_lst))
            TaskSchedule._raw_batch_upsert(connect, values)
        connect.close()
        return True

    @staticmethod
    def _raw_batch_upsert(connect, values):
        values_map = ['("%s", "%s", %s, %s, %s)' % item for item in values]
        sql = 'insert into task_schedule (task_schedule.key, kind, site, status, dealtime) values %s;' % ",".join(values_map)
        sql = text(sql)
        cursor = connect.execute(sql)
        cursor.close()
        return True

    @staticmethod
    def raw_upsert(connect, key, kind, site, **kwargs):
        dealtime = int(time.time()) + TaskSchedule.STEP
        value = kwargs.get("value", "")
        body = kwargs.get("body", "")
        # sql = 'insert into task_schedule (task_schedule.key, kind, site, status, dealtime) values ("%s", "%s", %s, %s, %s) on duplicate key update dealtime=%s' % (key, kind, site, TaskSchedule.INIT, dealtime, dealtime)
        sql = text('insert into task_schedule (task_schedule.key, kind, site, status, dealtime, task_schedule.value, body) values (:ts_key,:kind,:site,:status,:dealtime,:ts_value,:body) on duplicate key update dealtime=:dealtime,status=:status;')
        cursor = connect.execute(sql, ts_key=key, kind=kind, site=site, dealtime=dealtime, status=TaskSchedule.INIT, ts_value=value, body=body)
        cursor.close()
        return True

    @staticmethod
    def clear():
        connect = db.connect()
        sql = text("delete from task_schedule;")
        cursor = connect.execute(sql)
        cursor.close()
        connect.close()
        return True