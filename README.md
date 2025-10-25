# Stream 数据流处理管道

一个函数式编程风格的数据处理管道库，支持多种数据处理操作。

## 特性

- **链式调用**：通过 `|` 操作符实现流畅的链式调用
- **多种处理模式**：支持单条处理、批量处理、过滤、合并、连接等操作
- **操作符重载**：使用直观的操作符表示不同的处理模式
- **函数式编程**：无状态、无副作用的设计理念
- **批处理支持**：自动根据函数参数控制批处理大小
- **灵活的连接模式**：支持传统双函数和单函数连接模式
- **预处理连接**：支持在连接前对数据进行预处理
- **易于扩展**：可轻松添加新的处理模式和操作符

## 当前测试覆盖率报告：

```
Name                     Stmts   Miss  Cover   Missing
------------------------------------------------------
antchain/__init__.py         4      0   100%
antchain/core.py            84     23    73%   45-48, 53-59, 77, 122, 248-258, 293, 302-303, 307, 311, 315, 319, 323, 327, 331
antchain/strategies.py     168     71    58%   27, 52-58, 64, 78, 86-107, 112-152, 157-187, 194, 218, 248, 253, 274, 278-281, 288, 293-300, 307, 311-315, 322, 327-336, 373, 378, 388
antchain/utils.py           68     54    21%   21-41, 58-72, 86-152, 170-171
------------------------------------------------------
TOTAL                      324    148    54%
```

## 安装

```bash
pip install antchain
```

## 快速开始

```python
"""
数据流处理管道使用示例

展示如何使用stream包中的各种操作符和功能。
"""

import random
import re
from antchain import DATA, Start,COUNT, SET


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


def last_active_time(rows,stream_size=2):
    return [
        {"id": 1, "last_active_time": random.randint(1, 1000)},
        {"id": 2, "last_active_time": random.randint(1, 1000)},
        {"id": 3, "last_active_time": random.randint(1, 1000)},
    ]

# 连接条件
def join(
    left_key=lambda x: x["id"],
    right_key=lambda x: x["id"],
    left_property='active_info',
    one_to_many=False,
):
 pass

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
    | ((DATA & last_active_time) * join)  # 查询出用户活跃时间并关联到用户信息中,每次循环处理2条数据根据ID匹配
    | (DATA + init_vip)  # 添加vip用户到列表中
)

# 操作
print(chain())

# 统计总人数
cnt=chain | COUNT
print("总人数: " + str(cnt()))

# 查询活跃用户
active_user=chain | (DATA -(lambda r: r["status"] == "活跃"))
print("活跃用户: " + str(active_user()))

# 获取用户ID
ids = chain | (DATA > (lambda r: r["id"])) | SET
print("用户ID: " + str(ids()))

# 获取最大年龄的用户信息
max_age_user = chain | DATA >> (lambda rows: max(rows, key=lambda r: r["age"]))
print("最大年龄: " + str(max_age_user()))
```

## 操作符说明

### 基本操作符

| 操作符 | 语法 | 说明 |
|-------|------|------|
| `>` | `DATA > func` | 单条处理：将列表中的每个元素单独传递给函数处理 |
| `>>` | `DATA >> func` | 批量处理：将列表直接传递给函数处理,stream_size参数控制批次传递数量 |
| `-` | `DATA - func` | 过滤处理：过滤掉函数返回False的元素 |
| `+` | `DATA + func` | 合并处理：将函数返回的数据与现有数据合并,相当于合并两个列表 |
| `&` | `DATA & func` | 预处理：在连接操作前对数据进行预处理 |

### 连接操作符

| 操作符 | 语法 | 说明 |
|-------|------|------|
| `*` | `(DATA & right_data_func) * join_func` | 左连接（预处理模式）：先调right_data_func,再调join_func进行连接数据 |
| `**` | `(DATA & right_data_func) ** join_func` | 全连接（预处理模式）：先调right_data_func,再调join_func进行连接数据 |

## 批处理功能

Stream库支持自动批处理功能。当使用 `>>`、`*`、`**` 操作符时，系统会自动从函数参数中提取批处理大小：

1. 查找函数的 `stream_size` 参数默认值
2. 如果该参数存在且是大于0的整数，则用作批处理大小
3. 如果没有该参数或参数无效，则按全量处理

### 批处理示例

```python
from antchain import DATA, Start, COUNT

def init_data():
    return [{"id": i} for i in range(1, 101)]  # 100条数据

def process_items(items, stream_size=10):
    """批处理大小为10"""
    print(f"处理{len(items)}条数据")
    return items

# 自动按批处理大小10进行处理
stream = Start() | init_data | (DATA >> process_items) | COUNT
result = stream()  # 将分10批处理，每批10条数据
```

### 常用方法:
#### - PEEK: 用于查看数据,会打印当前数据
#### - LIST: 将结果转换为列表
#### - SET: 将结果转换为集合
#### - TUPLE: 将结果转换为元组
#### - FIRST: 获取结果中的第一个元素
#### - LAST: 获取结果中的最后一个元素
#### - NON: 用于过滤数据,返回非None数据
#### - COUNT: 统计数量
## 使用示例

### 1. 基本数据处理

```python
from antchain import DATA, StreamStart

def get_data():
    return [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]

def process_item(item):
    return {**item, "processed": True}

stream = StreamStart()
result = stream | get_data | (DATA > process_item)
print(result())  # [{'id': 1, 'name': 'Alice', 'processed': True}, ...]
```

### 2. 数据过滤

```python
def is_even_id(item):
    return item["id"] % 2 == 0

result = stream | get_data | (DATA - is_even_id)
print(result())  # 只保留id为偶数的记录
```

### 3. 数据合并

```python
def get_more_data():
    return [{"id": 3, "name": "Charlie"}]

result = stream | get_data | (DATA + get_more_data)
print(result())  # 合并两个数据集
```

### 4. 数据连接

```python
def get_teacher_data(rows,stream_size=1):
    return [{"id": 1, "teacher": "Mr. Smith"}, {"id": 2, "teacher": "Ms. Johnson"}]


# 连接条件
def join(
    left_key=lambda x: x["id"],
    right_key=lambda x: x["user_id"],
    left_property='address',
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


# 左连接 - 传统语法
result = (
    stream 
    | get_data 
    | (DATA & get_teacher_data) * join)
)


# 全连接 - 传统语法
result = (
    stream 
    | get_data 
    | (DATA & get_teacher_data) ** join)
)

```

## 线程安全性

`DATA` 本身是线程安全的，因为它是无状态对象。整个数据流管道的线程安全性取决于用户传入的处理函数是否线程安全。

建议：
1. 保持处理函数为纯函数（无副作用）
2. 避免在处理函数中修改共享状态
3. 使用线程安全的数据结构处理共享数据

## 许可证

MIT