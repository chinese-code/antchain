"""
数据流处理核心模块

该模块实现了数据流处理管道的核心功能，包括操作符重载、
链式调用、多种数据处理模式等。
"""

from typing import Any, Callable, Tuple, Union, Optional
import inspect
from .strategies import StrategyFactory


class OPMode:
    """
    操作模式标记类

    该类通过重载操作符来支持不同的数据处理模式。
    """

    def __init__(self, mode: Optional[str] = None):
        """
        初始化操作模式

        Args:
            mode: 操作模式标识符
        """
        self.mode = mode

    def __gt__(self, func: Callable) -> Tuple[str, Callable]:
        """
        重载 > 操作符，表示单条数据处理模式

        Args:
            func: 处理函数

        Returns:
            包含模式标识和处理函数的元组

        Example:
            DATA > process_func
        """
        return ("one", func)

    def __rshift__(self, func: Callable) -> Tuple[str, Callable]:
        """
        重载 >> 操作符，表示批量数据处理模式

        Args:
            func: 处理函数

        Returns:
            包含模式标识和处理函数的元组

        Example:
            DATA >> process_func
        """
        return ("list", func)

    def __sub__(self, func: Callable) -> Tuple[str, Callable]:
        """
        重载 - 操作符，表示过滤数据处理模式

        Args:
            func: 过滤函数，返回布尔值

        Returns:
            包含模式标识和过滤函数的元组

        Example:
            DATA - filter_func
        """
        return ("filter", func)

    def __add__(self, func: Callable) -> Tuple[str, Callable]:
        """
        重载 + 操作符，表示合并数据处理模式

        Args:
            func: 返回新数据的函数

        Returns:
            包含模式标识和合并函数的元组

        Example:
            DATA + merge_func
        """
        return ("merge", func)

    def __mul__(
        self, other: Union[Tuple[Callable, Callable], Callable]
    ) -> Tuple[str, Callable, Callable]:
        """
        重载 * 操作符，表示左连接数据处理模式

        Args:
            other: 包含条件函数和数据函数的元组 (condition_func, data_func) 或单个函数

        Returns:
            包含模式标识、条件函数和数据函数的元组

        Example:
            DATA * (condition_func, data_func)
            DATA * data_func  # data_func 返回数据或从stream_join参数获取条件
        """
        if callable(other):
            # 单函数模式：函数返回数据，条件从stream_join参数获取
            def extract_condition_data():
                # 获取函数的stream_join参数
                condition_func = None
                try:
                    sig = inspect.signature(other)
                    if "stream_join" in sig.parameters:
                        param = sig.parameters["stream_join"]
                        if param.default != inspect.Parameter.empty and callable(
                            param.default
                        ):
                            condition_func = param.default
                except (ValueError, TypeError):
                    pass

                # 执行函数获取数据
                data = other()
                return condition_func, data

            # 创建包装函数来提取条件函数和数据函数
            def condition_func_wrapper(left, right):
                # 这个函数实际上不会被直接调用，只是作为占位符
                # 真正的条件函数会在策略中提取
                pass

            def data_func_wrapper():
                # 这个函数实际上不会被直接调用，只是作为占位符
                # 真正的数据会在策略中提取
                pass

            # 将提取函数附加到包装器上，以便策略可以访问
            condition_func_wrapper._extract_func = extract_condition_data
            data_func_wrapper._extract_func = extract_condition_data

            return ("left_join", condition_func_wrapper, data_func_wrapper)
        elif isinstance(other, tuple) and len(other) == 2:
            condition_func, data_func = other
            return ("left_join", condition_func, data_func)
        else:
            raise TypeError(
                "左连接操作需要提供条件函数和数据函数: DATA * (condition_func, data_func) 或 DATA * func"
            )

    def __pow__(
        self, other: Union[Tuple[Callable, Callable], Callable]
    ) -> Tuple[str, Callable, Callable]:
        """
        重载 ** 操作符，表示全连接数据处理模式

        Args:
            other: 包含条件函数和数据函数的元组 (condition_func, data_func) 或单个函数

        Returns:
            包含模式标识、条件函数和数据函数的元组

        Example:
            DATA ** (condition_func, data_func)
            DATA ** data_func  # data_func 返回数据或从stream_join参数获取条件
        """
        if callable(other):
            # 单函数模式：函数返回数据，条件从stream_join参数获取
            def extract_condition_data():
                # 获取函数的stream_join参数
                condition_func = None
                try:
                    sig = inspect.signature(other)
                    if "stream_join" in sig.parameters:
                        param = sig.parameters["stream_join"]
                        if param.default != inspect.Parameter.empty and callable(
                            param.default
                        ):
                            condition_func = param.default
                except (ValueError, TypeError):
                    pass

                # 执行函数获取数据
                data = other()
                return condition_func, data

            # 创建包装函数来提取条件函数和数据函数
            def condition_func_wrapper(left, right):
                # 这个函数实际上不会被直接调用，只是作为占位符
                # 真正的条件函数会在策略中提取
                pass

            def data_func_wrapper():
                # 这个函数实际上不会被直接调用，只是作为占位符
                # 真正的数据会在策略中提取
                pass

            # 将提取函数附加到包装器上，以便策略可以访问
            condition_func_wrapper._extract_func = extract_condition_data
            data_func_wrapper._extract_func = extract_condition_data

            return ("full_join", condition_func_wrapper, data_func_wrapper)
        elif isinstance(other, tuple) and len(other) == 2:
            condition_func, data_func = other
            return ("full_join", condition_func, data_func)
        else:
            raise TypeError(
                "全连接操作需要提供条件函数和数据函数: DATA ** (condition_func, data_func) 或 DATA ** func"
            )


class Stream:
    """
    数据流核心类

    支持通过 | 运算符进行链式调用，自动识别不同的操作模式并切换处理逻辑。
    """

    def __init__(
        self, func: Callable, size: int = 100, process_mode: Optional[str] = None
    ):
        """
        初始化数据流

        Args:
            func: 处理函数
            size: 批处理大小
            process_mode: 处理模式
        """
        self.func = func
        self.batch_size = size
        self.process_mode = process_mode

    def __call__(self, *args, **kwargs) -> Any:
        """
        执行函数，返回最终结果（延迟执行）

        Args:
            *args: 位置参数
            **kwargs: 关键字参数

        Returns:
            函数执行结果
        """
        return self.func(*args, **kwargs)

    def __or__(self, other) -> "Stream":
        """
        重载 | 运算符，处理函数和操作符标记

        Args:
            other: 另一个函数或操作符

        Returns:
            新的Stream实例

        Raises:
            TypeError: 当不支持的操作类型时抛出
        """
        # 1. 处理操作符标记：使用策略模式
        if isinstance(other, tuple) and other[0] in (
            "one",
            "list",
            "filter",
            "merge",
            "left_join",
            "full_join",
        ):
            # 使用策略工厂创建处理策略
            strategy = StrategyFactory.create_strategy(other)

            # 定义"带处理策略的组合函数"
            def composed_with_strategy(*args, **kwargs):
                # 先执行前序函数，得到数据
                prev_result = self.func(*args, **kwargs)
                # 使用策略处理数据
                return strategy.process(prev_result, *args, **kwargs)

            # 返回新Stream，携带当前处理逻辑
            return Stream(composed_with_strategy, self.batch_size)

        # 2. 处理普通函数：默认批量传递
        elif callable(other):
            # 定义"批量组合函数"：前序结果直接传递给当前函数
            def composed_simple(*args, **kwargs):
                prev_result = self.func(*args, **kwargs)
                return other(prev_result)

            return Stream(composed_simple, self.batch_size)

        # 3. 不支持的类型
        else:
            raise TypeError(f"| 不支持 {type(self)} 与 {type(other)} 运算")


class Start:
    """
    数据流起始类

    用于启动链式调用，生成第一个Stream实例。
    """

    def __init__(self, size: int = 100):
        """
        初始化数据流起始点

        Args:
            size: 批处理大小
        """
        self.size = size

    def __or__(self, other) -> Stream:
        """
        重载 | 运算符，用于启动链式调用

        Args:
            other: 初始处理函数

        Returns:
            Stream实例

        Raises:
            TypeError: 当不支持的操作类型时抛出
        """
        # 起始时仅接收普通函数，生成初始Stream
        if callable(other):
            return Stream(other, self.size)
        raise TypeError(f"StreamStart | 不支持 {type(other)} 类型")


# 统一的数据处理操作符
DATA = OPMode(None)


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
