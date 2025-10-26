import random
from typing import Any
from antchain import Start, DATA
from antchain.stream import NON


def init():
    return [
        {"id": 1, "name": "张三"},
        {"id": 2, "name": "张三"},
        {"id": 3, "name": "张三"},
        {"id": 4, "name": "张三"},
        {"id": 5, "name": "张三"},
        {"id": 6, "name": "张三"},
        {"id": 7, "name": "张三"},
        {"id": 8, "name": "张三"},
        {"id": 9, "name": "张三"},
    ]


def add_age(row):
    row["age"] = random.randint(10, 30)
    return row


def user_detail(rows, stream_size=1):
    return [
        {"id": 1, "nick": "张三"},
        {"id": 2, "nick": "张三"},
        {"id": 3, "nick": "张三"},
        {"id": 4, "nick": "张三"},
        {"id": 5, "nick": "张三"},
        {"id": 6, "nick": "张三"},
        {"id": 7, "nick": "张三"},
        {"id": 8, "nick": "张三"},
        {"id": 9, "nick": "张三"},
    ]


def left_join_user(left_key=lambda x: x.id, right_key=lambda x: x.id):
    pass


def each(row):
    print("一行数据")
    print(row)
    return row


def show(rows, stream_size=3):
    print(rows)
    return rows


def id_filter(row) -> bool:
    return row["id"] % 2 == 0


def add_user():
    return [
        {"id": 12, "nick": "张三"},
        {"id": 13, "nick": "张三"},
    ]


def add_one_user():
    return {"id": 12, "nick": "张三"}


def add_none() -> None:
    return None


def right_data(rows, stream_size=3):
    return [{"user_id": 4, "address": "上海"}, {"user_id": 5, "address": "北京"}]


def left_join(
    left_key=lambda x: x["id"],
    right_key=lambda x: x["user_id"],
    left_property="address",
    one_to_many=False,
):
    """
    left_key: 左边key
    right_key: 右边key
    left_property: 把右边数据挂到左边数据的这个属性名上,为空时,
                        会将数据放到左边数据中,如果有重名的属性,左边的重名属性数据会被覆盖
    one_to_many: False  一对一,True 一对多,
                        当一对多有property_name不为空时,会生成一个列表,列表中存放所有匹配的数据,
                        如果没有property_name,则会重复生成多条左边的数据.然后与右边一对一.
    """
    pass


start = Start()

chain1 = (
    start
    | init
    | (DATA > add_age)  # 添加年龄
    | (DATA - id_filter)  # 过滤id为1的数据
    | (DATA + add_user)  # 添加多个用户
    | (DATA + add_one_user)  # 添加一个用户
    | (DATA + add_none)  # 添加一个空数据
    | NON  # 过滤掉None数据
    | (DATA & right_data) * left_join  # 左外连接
)


def init2():
    return [{"id": 4, "email": "tumingjian@163.com"}]


def chain1_eft_join(
    left_key=lambda x: x["id"],
    right_key=lambda x: x["id"],
    left_property=None,
    one_to_many=False,
):
    pass


print("chain2 ----start")
# 串联chian1,实际可以一直串联下去
chain = start | init2 | (DATA & chain1) * chain1_eft_join

dt = chain()
print(dt)
print("chain2 ----end")
