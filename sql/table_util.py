#coding=utf8
__author__ = 'changdongsheng'
from sql.base import db, metadata
from sql.category import Category
from sql.joom_pro import JoomPro
from sql.joom_review import JoomReview
from sql.joom_shop import JoomShop
from sql.joom_user import JoomUser
from sql.product_body import ProductBody
from sql.task_schedule import TaskSchedule


def create_all_tables():
    """
    创建所有表
    """
    metadata.create_all(bind=db)


if __name__ == "__main__":
    create_all_tables()