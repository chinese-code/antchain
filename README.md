# Stream 数据流处理管道

一个函数式编程风格的数据处理管道库，支持多种数据处理操作。

## 特性

- **链式调用**：通过 `|` 操作符实现流畅的链式调用
- **多种处理模式**：支持单条处理、批量处理、过滤、合并、连接等操作
- **操作符重载**：使用直观的操作符表示不同的处理模式
- **函数式编程**：无状态、无副作用的设计理念
- **批处理支持**：自动根据函数参数控制批处理大小
- **灵活的连接模式**：支持传统双函数和单函数连接模式
- **易于扩展**：可轻松添加新的处理模式和操作符

## 安装

```bash
# 该包为本地包，直接在项目中使用即可
```

## 快速开始

```python
from stream import DATA, Stream

# 定义处理函数
def init_data():
    return [{"id": 1}, {"id": 2}]

def process_item(item):
    return {"id": item["id"], "name": f"Item {item['id']}"}

def filter_item(item):
    return item["id"] % 2 == 0

# 创建数据流起始点
stream = Stream()

# 构建处理管道
result = (
    stream 
    | init_data 
    | (DATA > process_item)    # 单条处理
    | (DATA - filter_item)     # 过滤处理
)

# 执行处理
print(result())
```

## 操作符说明

### 基本操作符

| 操作符 | 语法 | 说明 |
|-------|------|------|
| `>` | `DATA > func` | 单条处理：将列表中的每个元素单独传递给函数处理 |
| `>>` | `DATA >> func` | 批量处理：将整个列表直接传递给函数处理 |
| `-` | `DATA - func` | 过滤处理：过滤掉函数返回False的元素 |
| `+` | `DATA + func` | 合并处理：将函数返回的数据与现有数据合并 |

### 连接操作符

| 操作符 | 语法 | 说明 |
|-------|------|------|
| `*` | `DATA * (condition_func, data_func)` | 左连接：基于条件函数连接两个数据集，只保留左侧数据 |
| `*` | `DATA * data_func` | 左连接（单函数模式）：从data_func的stream_join参数获取条件函数 |
| `**` | `DATA ** (condition_func, data_func)` | 全连接：基于条件函数连接两个数据集，保留所有数据 |
| `**` | `DATA ** data_func` | 全连接（单函数模式）：从data_func的stream_join参数获取条件函数 |

## 批处理功能

Stream库支持自动批处理功能。当使用 `>>`、`*`、`**` 操作符时，系统会自动从函数参数中提取批处理大小：

1. 查找函数的 `stream_size` 参数默认值
2. 如果该参数存在且是大于0的整数，则用作批处理大小
3. 如果没有该参数或参数无效，则按全量处理

### 批处理示例

```python
from stream import DATA, Start, COUNT

def init_data():
    return [{"id": i} for i in range(1, 101)]  # 100条数据

def process_items(items, stream_size=10):
    """批处理大小为10"""
    print(f"处理{len(items)}条数据")
    return items

# 自动按批处理大小10进行处理
stream = Start() | init_data | (DATA >> process_items) | COUNT
result = stream()  # 将分10批处理，每批10条数据

def join_data_func(items,stream_join=condition_func, stream_size=5):
    """左连接批处理大小为5，连接条件从stream_join参数获取"""
    return [{"id": 1, "info": "data1"}]

# 左连接时按批处理大小5进行处理
stream = Start() | init_data | (DATA * join_data_func) | COUNT
```

## 连接操作的单函数模式

Stream库支持通过函数参数获取连接条件的单函数模式，使代码更加简洁：

```python
def join_condition(left, right):
    return left["id"] == right["id"]

def join_data_func(stream_join=join_condition):
    """通过stream_join参数获取连接条件"""
    return [
        {"id": 1, "info": "data1"},
        {"id": 2, "info": "data2"},
    ]

# 使用单函数模式进行左连接
stream = Start() | init_data | (DATA * join_data_func) | LIST

# 使用单函数模式进行全连接
stream = Start() | init_data | (DATA ** join_data_func) | LIST
```

## 使用示例

### 1. 基本数据处理

```python
from stream import DATA, StreamStart

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
def get_teacher_data():
    return [{"id": 1, "teacher": "Mr. Smith"}, {"id": 2, "teacher": "Ms. Johnson"}]

def join_condition(student, teacher):
    return student["id"] == teacher["id"]

# 左连接 - 传统语法
result = (
    stream 
    | get_data 
    | (DATA * (join_condition, get_teacher_data))
)

# 左连接 - 单函数语法
def join_data_func(items,stream_join=join_condition):
    return (get_teacher_data())
    
result = (
    stream 
    | get_data 
    | (DATA * join_data_func)
)

# 全连接 - 传统语法
result = (
    stream 
    | get_data 
    | (DATA ** (join_condition, get_teacher_data))
)

# 全连接 - 单函数语法
def join_data_func(items,stream_join=join_condition):
    return (get_teacher_data())
    
result = (
    stream 
    | get_data 
    | (DATA ** join_data_func)
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