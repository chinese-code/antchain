
from antchain import Start,DATA

def init():
    print("这是一个函数")
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


def user_detail(rows, stream_size=1):
    print("这是一个函数")
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


def show(rows):
    print(rows)
    return rows


start = Start()

chain = (
    start
    | init
    | (DATA & user_detail) * left_join_user
    | (DATA > each)
    | (DATA >> show)
)

dt = chain()
