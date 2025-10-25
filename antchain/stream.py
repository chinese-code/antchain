from typing import Any, Callable
from .strategy import StrategyFactory
from .element import Element





class Stream:

    def __init__(self, mode: str, element: Element):
        self.mode = mode
        self.element = element
        self.child_nodes = list[Stream]()
        pass

    def __or__(self, other: Element):
        stream = Stream("next", element=other)
        self.child_nodes.append(stream)
        return self

    def __call__(self, *args, **kwds):
        return self.process(self)

    def process(self, stream: "Stream") -> Any:
        data = StrategyFactory.process(stream.element)
        if self.child_nodes is not None and len(self.child_nodes) > 0:
            for node in self.child_nodes:
                node.element.left_data = data
                data = StrategyFactory.process(node.element)
        return data


class Start:

    def __init__(self):
        pass

    def __or__(self, other):
        print("start |")
        if callable(other):
            print("函数:" + other.__name__)
        return Stream("init", Element(element_type="init", right_func=other))


# 统一的数据处理操作符
DATA = Element(element_type="data")


# 一般用来DEBUG观察数据
def peek(rows):
    print(rows)
    return rows


def collect_list(rows):
    return rows


def collect_set(rows):
    return set(rows)


def collect_count(rows):
    return len(rows)


def collect_tuple(rows):
    return tuple(rows)


def collect_first(rows):
    return rows[0] if len(rows) > 0 else None


def collect_last(rows):
    return rows[-1] if len(rows) > 0 else None


def filter_none(row):
    return row is not None


# DEBUG 模式，打印并返回数据
PEEK = DATA >> peek
# 转成为列表
LIST = DATA >> collect_list
# 转成为set集合,可以作为去重使用
SET = DATA >> collect_set
# 计数
COUNT = DATA >> collect_count
# 转成为元组
TUPLE = DATA >> collect_tuple
# 取第一个
FIRST = DATA >> collect_first
# 取最后一个
LAST = DATA >> collect_last
# 过滤为None的数据,也就是保留不为None的数据
NON = DATA - filter_none
