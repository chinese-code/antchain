"""
Stream模块

该模块定义了数据流的核心类，包括Stream和Start类，以及各种常用的收集器函数。
Stream类支持链式调用，通过|操作符连接不同的处理步骤。
"""

from typing import Any, Callable, Dict, List, Tuple, Union
from .strategy import StrategyFactory
from .element import Element
from .exceptions import ProcessingError


def collect_list(rows: Any) -> Any:
    """
    收集数据为列表

    Args:
        rows (Any): 数据

    Returns:
        list: 数据列表
    """
    return list(rows) if isinstance(rows, (list, tuple)) else [rows]


def collect_set(rows: Any) -> set:
    """
    收集数据为集合

    Args:
        rows (Any): 数据

    Returns:
        set: 数据集合
    """
    if isinstance(rows, (list, tuple)):
        return set(rows)
    else:
        return {rows}


def collect_count(rows: Any) -> int:
    """
    计算数据数量

    Args:
        rows (Any): 数据

    Returns:
        int: 数据数量
    """
    if isinstance(rows, (list, tuple)):
        return len(rows)
    else:
        return 1 if rows is not None else 0


def collect_tuple(rows: Any) -> tuple:
    """
    收集数据为元组

    Args:
        rows (Any): 数据

    Returns:
        tuple: 数据元组
    """
    if isinstance(rows, (list, tuple)):
        return tuple(rows)
    else:
        return (rows,)


def collect_first(rows: Any) -> Any:
    """
    获取第一个数据

    Args:
        rows (Any): 数据列表

    Returns:
        Any: 第一个数据，如果列表为空则返回None
    """
    if isinstance(rows, (list, tuple)) and len(rows) > 0:
        return rows[0]
    elif isinstance(rows, (list, tuple)) and len(rows) == 0:
        return None
    elif rows is not None:
        return rows
    else:
        return None


def collect_last(rows: Any) -> Any:
    """
    获取最后一个数据

    Args:
        rows (Any): 数据列表

    Returns:
        Any: 最后一个数据，如果列表为空则返回None
    """
    if isinstance(rows, (list, tuple)) and len(rows) > 0:
        return rows[-1]
    elif isinstance(rows, (list, tuple)) and len(rows) == 0:
        return None
    elif rows is not None:
        return rows
    else:
        return None


def collect_max(rows: Any) -> Any:
    """
    获取数据中的最大值

    Args:
        rows (Any): 数据列表

    Returns:
        Any: 最大值，如果列表为空则返回None
    """
    if isinstance(rows, (list, tuple)) and len(rows) > 0:
        return max(rows)
    elif isinstance(rows, (list, tuple)) and len(rows) == 0:
        return None
    elif rows is not None:
        return rows
    else:
        return None


def collect_min(rows: Any) -> Any:
    """
    获取数据中的最小值

    Args:
        rows (Any): 数据列表

    Returns:
        Any: 最小值，如果列表为空则返回None
    """
    if isinstance(rows, (list, tuple)) and len(rows) > 0:
        return min(rows)
    elif isinstance(rows, (list, tuple)) and len(rows) == 0:
        return None
    elif rows is not None:
        return rows
    else:
        return None


def collect_sum(rows: Any) -> Union[int, float]:
    """
    计算数据的总和

    Args:
        rows (Any): 数字列表

    Returns:
        Any: 总和，如果列表为空则返回0
    """
    if isinstance(rows, (list, tuple)) and len(rows) > 0:
        return sum(rows)
    elif isinstance(rows, (int, float)):
        return rows
    else:
        return 0


def collect_avg(rows: Any) -> float:
    """
    计算数据的平均值

    Args:
        rows (Any): 数字列表

    Returns:
        float: 平均值，如果列表为空则返回0
    """
    if isinstance(rows, (list, tuple)) and len(rows) > 0:
        return sum(rows) / len(rows)
    elif isinstance(rows, (int, float)):
        return float(rows)
    else:
        return 0.0


class Stream:
    """
    数据流核心类

    该类表示数据流处理管道中的一个节点，支持链式调用。
    通过|操作符可以连接不同的处理步骤，形成完整的数据处理管道。
    """

    def __init__(self, mode: str, element: Element) -> None:
        """
        初始化Stream实例

        Args:
            mode (str): 模式
            element (Element): 元素
        """
        self.mode = mode
        self.element = element
        self.child_nodes: List[Stream] = list()
        pass

    def __or__(self, other: Element) -> "Stream":
        """
        | 操作符重载，用于连接下一个处理步骤

        Args:
            other (Element): 下一个处理元素

        Returns:
            Stream: 新的Stream实例
        """
        # 创建新的Stream对象，而不是修改当前对象
        new_stream = Stream(self.mode, self.element)
        # 复制现有的child_nodes
        new_stream.child_nodes = self.child_nodes.copy()
        # 创建下一个处理步骤的Stream对象
        next_stream = Stream("next", other)
        # 将下一个处理步骤添加到新Stream的child_nodes中
        new_stream.child_nodes.append(next_stream)
        return new_stream

    def __call__(self, *args: Any, **kwds: Any) -> Any:
        """
        调用操作符重载，执行整个数据流处理管道

        Returns:
            Any: 处理结果
        """
        return self.process(self)

    def process(self, stream: "Stream") -> Any:
        """
        处理数据流

        Args:
            stream (Stream): 要处理的数据流

        Returns:
            Any: 处理结果

        Raises:
            ProcessingError: 当数据流处理过程中出现异常时
        """
        try:
            data = StrategyFactory.process(stream.element)
            if self.child_nodes is not None and len(self.child_nodes) > 0:
                for node in self.child_nodes:
                    node.element.left_data = data
                    data = StrategyFactory.process(node.element)
            return data
        except Exception as e:
            raise ProcessingError(f"数据流处理过程中出现错误: {str(e)}") from e


class Start:
    """
    数据流起始类

    该类用于启动数据流处理管道，作为整个处理链的起点。
    """

    def __init__(self) -> None:
        """
        初始化Start实例
        """
        pass

    def __or__(self, other: Callable[..., Any]) -> Stream:
        """
        | 操作符重载，用于连接第一个处理步骤

        Args:
            other (Callable): 第一个处理函数

        Returns:
            Stream: Stream实例
        """
        # print("start |")
        if callable(other):
            # print("函数:" + other.__name__)
            pass
        return Stream("init", Element(element_type="init", right_func=other))


# 统一的数据处理操作符
DATA = Element(element_type="data")


# 一般用来DEBUG观察数据
def peek(rows: Any) -> Any:
    """
    查看数据函数，用于调试

    Args:
        rows (Any): 数据

    Returns:
        Any: 原始数据
    """
    print(rows)
    return rows


def filter_none(row: Any) -> bool:
    """
    过滤None值

    Args:
        row (Any): 数据项

    Returns:
        bool: 如果不是None则返回True
    """
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
# 获取最大值
MAX = DATA >> collect_max
# 获取最小值
MIN = DATA >> collect_min
# 求和
SUM = DATA >> collect_sum
# 求平均值
AVG = DATA >> collect_avg
