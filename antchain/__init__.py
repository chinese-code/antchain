"""
数据流处理管道包

该包提供了一套函数式编程风格的数据处理管道，支持多种数据处理操作，
包括单条处理、批量处理、过滤、合并、连接等操作。

主要组件：
- DATA: 操作符，提供各种数据处理操作符
- Stream: 数据流核心类，支持链式调用
- StreamStart: 数据流起始类，用于启动链式调用

常用方法:
- PEEK: 用于查看数据,会打印当前数据
- LIST: 将结果转换为列表
- SET: 将结果转换为集合
- TUPLE: 将结果转换为元组
- FIRST: 获取结果中的第一个元素
- LAST: 获取结果中的最后一个元素
- NON: 用于过滤数据,返回非None数据
- COUNT: 统计数量

使用示例：
    from stream import DATA, StreamStart

    def init():
        return [{"id": 1}, {"id": 2}]

    def process_item(item):
        return {"id": item["id"], "name": f"Item {item['id']}"}

    def filter_item(item):
        return item["id"] % 2 == 0

    stream_start = StreamStart()
    result = stream_start | init | (DATA > process_item) | (DATA - filter_item)
"""

from .stream import Start, DATA, PEEK, LIST, SET, COUNT, TUPLE, FIRST, LAST, NON

__all__ = [
    "Start",
    "DATA",
    "PEEK",
    "LIST",
    "SET",
    "COUNT",
    "TUPLE",
    "FIRST",
    "LAST",
    "NON",
]
__version__ = "0.0.7"
__author__ = "tumingjian@foxmail.com"
