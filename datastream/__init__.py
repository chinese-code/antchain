"""
数据流处理管道包

该包提供了一套函数式编程风格的数据处理管道，支持多种数据处理操作，
包括单条处理、批量处理、过滤、合并、连接等操作。

主要组件：
- DATA: 操作符，提供各种数据处理操作符
- Stream: 数据流核心类，支持链式调用
- StreamStart: 数据流起始类，用于启动链式调用

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

from .core import Start, DATA, PEEK, LIST, SET, COUNT, TUPLE, FIRST, LAST

__all__ = ["Start", "DATA", "PEEK", "LIST", "SET", "COUNT", "TUPLE", "FIRST", "LAST"]
__version__ = "0.0.1"
__author__ = "Developer"
