#coding=utf8
__author__ = 'changdongsheng'
import contextlib

from sql.base import db
from sqlalchemy.orm import sessionmaker
Session = sessionmaker(bind=db)


def get_session():
    return Session()


@contextlib.contextmanager
def sessionCM():
    session = get_session()
    try:
        yield session
    except Exception, e:
        raise
    finally:
        session.close()

