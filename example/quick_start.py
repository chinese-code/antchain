"""
数据流处理管道使用示例

展示如何使用stream包中的各种操作符和功能。
"""

import random
from antchain import DATA, Start, COUNT, SET


def init_user():
    return [
        {"id": 1, "name": "ww"},
        {"id": 2, "name": "mm"},
        {"id": 3, "name": "dd"},
        {"id": 4, "name": "ee"},
    ]


def add_age(row):
    """
    添加age字段
    """
    row["age"] = random.randint(10, 20)
    return row


def add_address(row):
    """
    添加address字段
    """
    row["address"] = random.choice(["北京", "上海", "广州"])
    return row


def modify_age(row):
    """
    修改age字段
    """
    row["age"] = row["age"] + 10
    return row


def add_status(rows):
    for row in rows:
        row["status"] = random.choice(["不活跃", "活跃"])
    return rows


def last_active_time(stream_size=2, stream_join=lambda r1, r2: r1["id"] == r2["id"]):
    return [
        {"id": 1, "last_active_time": random.randint(1, 1000)},
        {"id": 2, "last_active_time": random.randint(1, 1000)},
        {"id": 3, "last_active_time": random.randint(1, 1000)},
    ]


def init_vip():
    return [
        {
            "id": 10,
            "vip": True,
            "age": 33,
            "address": "纽约",
            "last_active_time": random.randint(1, 1000),
            "status": "活跃",
        }
    ]



# 开始标记
start = Start()
# 数据处理Stream
chain = (
    start
    | init_user  # 初始化数据
    | (DATA > add_age)  # 单条循环添加age字段
    | (DATA > add_address)  # 单条循环添加address字段
    | (DATA > modify_age)  # 单条循环修改age字段
    | (DATA >> add_status)  # 添加状态字段
    | (
        DATA * last_active_time
    )  # 查询出用户活跃时间并关联到用户信息中,每次循环处理2条数据根据ID匹配
    | (DATA + init_vip)  # 添加vip用户到列表中
)

# 操作
print(chain())

# 统计总人数
cnt = chain | COUNT
print("总人数: " + str(cnt()))

# 查询活跃用户
active_user = chain | (DATA - (lambda r: r["status"] == "活跃"))
print("活跃用户: " + str(active_user()))

# 获取用户ID
ids = chain | (DATA > (lambda r: r["id"])) | SET
print("用户ID: " + str(ids()))

# 获取最大年龄的用户信息
max_age_user = chain | DATA >> (lambda rows: max(rows, key=lambda r: r["age"]))
print("最大年龄: " + str(max_age_user()))
