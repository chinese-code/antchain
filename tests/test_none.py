
import random

# 更新为正确的模块名 antchain
from antchain import Start, DATA, LIST,NOT_NONE,PEEK


def init():
    return [{"id": 1, "name": "one"}, {"id": 2, "name": "tow"}]


def age(row):
    row["age"] = random.randint(10, 20)  # 修复：使用 randint 生成单个年龄值
    return row  # 修复：返回修改后的行


def address(row):
    row["address"] = random.choice(["北京", "上海", "广州"])
    return row  # 修复：返回修改后的行


def clean(row):
    print('clean')
    # 修复：根据函数名推测，应该是清理数据而不是返回 None
    # 这里我们保留有效字段并返回
    return None


# 创建数据流
start = Start()

# 构建处理链
chain = start | init | (DATA > age) | (DATA > address) | (DATA > clean) | PEEK | NOT_NONE | LIST

# 执行处理链
result = chain()

# 打印结果
print(result)
