"""
Element模块

该模块定义了数据流处理管道中的基本元素类，用于表示不同的数据处理操作。
Element类支持多种操作符重载，包括：
- &: 预处理操作符
- +: 合并操作符
- -: 过滤操作符
- *: 左连接操作符
- **: 全连接操作符
- >: 单条处理操作符
- >>: 批处理操作符

每个Element实例表示数据流中的一个处理步骤。
"""

from typing import Callable, Any
from .validators import validate_function_args_count, validate_filter_function


class Element:
    """
    数据流处理管道中的基本元素类

    Attributes:
        element_type (str): 元素类型，决定使用哪种处理策略
        left_data (Any): 左侧数据，用于连接操作
        right_func (Callable | None): 右侧函数，具体的处理函数
        join_func (Callable | None): 连接函数，用于连接操作
    """

    def __init__(
        self,
        element_type: str = "none",
        left_data: Any = None,
        right_func: Callable | None = None,
        join_func: Callable | None = None,
    ) -> None:
        """
        初始化Element实例

        Args:
            element_type (str): 元素类型，默认为"none"
            left_data (Any): 左侧数据，默认为None
            right_func (Callable | None): 右侧处理函数，默认为None
            join_func (Callable | None): 连接函数，默认为None
        """
        self.left_data: None | list | tuple | Any = None
        self.element_type = element_type
        self.right_func = right_func
        self.join_func = join_func

    def __and__(self, other: Callable[..., Any]) -> "Element":
        """
        & 操作符重载，用于预处理操作

        Args:
            other (Callable): 预处理函数

        Returns:
            Element: 新的Element实例，类型为"and"
        """
        return Element(element_type="and", right_func=other)

    def __add__(self, other: Callable[..., Any]) -> "Element":
        """
        + 操作符重载，用于合并操作

        Args:
            other (Callable): 合并函数

        Returns:
            Element: 新的Element实例，类型为"merge"
        """
        return Element(element_type="merge", right_func=other)

    def __sub__(self, other: Callable[..., Any]) -> "Element":
        """
        - 操作符重载，用于过滤操作

        Args:
            other (Callable): 过滤函数，应返回bool值

        Returns:
            Element: 新的Element实例，类型为"filter"

        Raises:
            ValidationError: 当函数参数或返回值类型不符合要求时
        """
        validate_filter_function(other)
        return Element(element_type="filter", right_func=other)

    def __mul__(self, other: Callable[..., Any]) -> "Element":
        """
        * 操作符重载，用于左连接操作

        Args:
            other (Callable): 连接函数

        Returns:
            Element: 当前Element实例，类型更新为"left_join"
        """
        self.element_type = "left_join"
        self.join_func = other
        return self

    def __pow__(self, other: Callable[..., Any]) -> "Element":
        """
        ** 操作符重载，用于全连接操作

        Args:
            other (Callable): 连接函数

        Returns:
            Element: 当前Element实例，类型更新为"all_join"
        """
        self.element_type = "all_join"
        self.join_func = other
        return self

    def __gt__(self, other: Callable[..., Any]) -> "Element":
        """
        > 操作符重载，用于单条处理操作

        Args:
            other (Callable): 处理函数，应接受单个参数

        Returns:
            Element: 新的Element实例，类型为"one"

        Raises:
            ValidationError: 当函数参数个数不符合要求时
        """
        validate_function_args_count(other, 1, "单条处理")
        return Element(element_type="one", right_func=other)

    def __rshift__(self, other: Callable[..., Any]) -> "Element":
        """
        >> 操作符重载，用于批处理操作

        Args:
            other (Callable): 处理函数，应接受列表参数

        Returns:
            Element: 新的Element实例，类型为"multi"

        Raises:
            ValidationError: 当函数参数个数不符合要求时
        """
        validate_function_args_count(other, 1, "批处理")
        return Element(element_type="multi", right_func=other)
