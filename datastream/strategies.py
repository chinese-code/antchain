"""
数据流处理策略模块

该模块实现了各种数据处理模式的策略类，用于替代Stream.__or__方法中的
多层if/elif结构，提高代码的可维护性和扩展性。
"""

from typing import Any, Callable, List, Tuple, Set, Dict
from abc import ABC, abstractmethod
from .utils import extract_batch_size, batch_process


class ProcessingStrategy(ABC):
    """
    数据处理策略抽象基类

    定义了所有处理策略必须实现的接口。
    """

    @abstractmethod
    def process(self, prev_result: Any, *args, **kwargs) -> Any:
        """
        处理数据的核心方法

        Args:
            prev_result: 前一个处理步骤的结果
            *args: 位置参数
            **kwargs: 关键字参数

        Returns:
            处理后的结果
        """
        pass


class SingleItemStrategy(ProcessingStrategy):
    """单条数据处理策略 (>)"""

    def __init__(self, func: Callable):
        self.func = func

    def process(self, prev_result: Any, *args, **kwargs) -> Any:
        """处理单条数据"""
        if isinstance(prev_result, list):
            return [self.func(item) for item in prev_result]
        else:
            return self.func(prev_result)


class BatchStrategy(ProcessingStrategy):
    """批量数据处理策略 (>>)"""

    def __init__(self, func: Callable):
        self.func = func

    def process(self, prev_result: Any, *args, **kwargs) -> Any:
        """批量处理数据，支持按函数参数中的stream_size进行批处理"""
        # 从函数参数中提取批处理大小，默认为0（全量处理）
        batch_size = extract_batch_size(self.func, 0)

        if isinstance(prev_result, list):
            # 如果是列表，进行批处理
            return batch_process(prev_result, batch_size, self.func, *args, **kwargs)
        else:
            # 单条数据直接处理
            return self.func(prev_result)


class FilterStrategy(ProcessingStrategy):
    """过滤数据处理策略 (-)"""

    def __init__(self, func: Callable):
        self.func = func

    def process(self, prev_result: Any, *args, **kwargs) -> Any:
        """过滤数据"""
        if isinstance(prev_result, list):
            return [item for item in prev_result if self.func(item)]
        else:
            # 单条数据，如果返回True则保留，否则返回空列表
            return prev_result if self.func(prev_result) else []


class MergeStrategy(ProcessingStrategy):
    """合并数据处理策略 (+)"""

    def __init__(self, func: Callable):
        self.func = func

    def process(self, prev_result: Any, *args, **kwargs) -> Any:
        """合并数据"""
        # 执行目标函数获取新数据
        new_data = self.func()
        # 合并数据
        if isinstance(prev_result, list) and isinstance(new_data, list):
            return prev_result + new_data
        elif isinstance(prev_result, list):
            return prev_result + [new_data]
        elif isinstance(new_data, list):
            return [prev_result] + new_data
        else:
            return [prev_result, new_data]


class LeftJoinStrategy(ProcessingStrategy):
    """左连接数据处理策略 (*)"""

    def __init__(self, condition_func: Callable, data_func: Callable):
        self.condition_func = condition_func
        self.data_func = data_func

    def process(self, prev_result: Any, *args, **kwargs) -> Any:
        """左连接处理，支持按函数参数中的stream_size进行批处理"""
        # 检查是否是单函数模式
        if hasattr(self.condition_func, "_extract_func"):
            # 从单函数中提取条件函数和数据
            condition_func, right_data = self.condition_func._extract_func()
        else:
            condition_func = self.condition_func
            right_data = (
                self.data_func() if callable(self.data_func) else self.data_func
            )

        # 确保左右数据都是列表
        left_data = prev_result if isinstance(prev_result, list) else [prev_result]
        right_data = right_data if isinstance(right_data, list) else [right_data]

        # 从函数参数中提取批处理大小，默认为0（全量处理）
        batch_size = extract_batch_size(self.data_func, 0)

        if batch_size > 0 and len(left_data) > batch_size:
            # 需要分批处理
            result = []
            for i in range(0, len(left_data), batch_size):
                batch_left = left_data[i : i + batch_size]
                # 对每个批次执行左连接
                batch_result = self._perform_left_join(
                    batch_left, right_data, condition_func
                )
                result.extend(batch_result)
            return result
        else:
            # 全量处理
            return self._perform_left_join(left_data, right_data, condition_func)

    def _perform_left_join(
        self, left_data: List[Any], right_data: List[Any], condition_func: Callable
    ) -> List[Any]:
        """执行左连接操作"""
        result = []
        for left_item in left_data:
            matched = False
            for right_item in right_data:
                # 只有当condition_func不为None时才使用它进行匹配
                if condition_func is None or condition_func(left_item, right_item):
                    # 合并两个字典
                    merged = {**left_item}
                    merged.update(right_item)
                    result.append(merged)
                    matched = True
                    break  # 左连接只取第一个匹配项
            if not matched:
                result.append(left_item)  # 没有匹配项时保留左侧数据
        return result


class FullJoinStrategy(ProcessingStrategy):
    """全连接数据处理策略 (**)"""

    def __init__(self, condition_func: Callable, data_func: Callable):
        self.condition_func = condition_func
        self.data_func = data_func

    def process(self, prev_result: Any, *args, **kwargs) -> Any:
        """全连接处理，支持按函数参数中的stream_size进行批处理"""
        # 检查是否是单函数模式
        if hasattr(self.condition_func, "_extract_func"):
            # 从单函数中提取条件函数和数据
            condition_func, right_data = self.condition_func._extract_func()
        else:
            condition_func = self.condition_func
            right_data = (
                self.data_func() if callable(self.data_func) else self.data_func
            )

        # 确保左右数据都是列表
        left_data = prev_result if isinstance(prev_result, list) else [prev_result]
        right_data = right_data if isinstance(right_data, list) else [right_data]

        # 从函数参数中提取批处理大小，默认为0（全量处理）
        batch_size = extract_batch_size(self.data_func, 0)

        if batch_size > 0 and len(left_data) > batch_size:
            # 需要分批处理
            result = []
            for i in range(0, len(left_data), batch_size):
                batch_left = left_data[i : i + batch_size]
                # 对每个批次执行全连接
                batch_result = self._perform_full_join(
                    batch_left, right_data, condition_func
                )
                result.extend(batch_result)
            return result
        else:
            # 全量处理
            return self._perform_full_join(left_data, right_data, condition_func)

    def _perform_full_join(
        self, left_data: List[Any], right_data: List[Any], condition_func: Callable
    ) -> List[Any]:
        """执行全连接操作"""
        # 执行全连接
        result = []
        matched_right: Set[int] = set()  # 记录右侧已匹配的项的索引

        # 处理左侧数据
        for left_item in left_data:
            matched = False
            for i, right_item in enumerate(right_data):
                # 只有当condition_func不为None时才使用它进行匹配
                if condition_func is None or condition_func(left_item, right_item):
                    # 合并两个字典
                    merged = {**left_item}
                    merged.update(right_item)
                    result.append(merged)
                    matched = True
                    matched_right.add(i)  # 记录已匹配的右侧项
                    break  # 一个左侧项只匹配一个右侧项
            if not matched:
                result.append(left_item)  # 没有匹配项时保留左侧数据

        # 添加右侧未匹配的数据
        for i, right_item in enumerate(right_data):
            if i not in matched_right:
                result.append(right_item)

        return result


class StrategyFactory:
    """
    策略工厂类

    根据操作符元组创建相应的处理策略实例。
    这个类是无状态的，因此是线程安全的。
    """

    # 策略映射表
    _strategies: Dict[str, type] = {
        "one": SingleItemStrategy,
        "list": BatchStrategy,
        "filter": FilterStrategy,
        "merge": MergeStrategy,
        "left_join": LeftJoinStrategy,
        "full_join": FullJoinStrategy,
    }

    @classmethod
    def create_strategy(cls, operation_tuple: Tuple) -> ProcessingStrategy:
        """
        根据操作符元组创建处理策略

        Args:
            operation_tuple: 包含操作类型和相关函数的元组

        Returns:
            相应的处理策略实例

        Raises:
            ValueError: 当操作类型不支持时抛出
        """
        if not isinstance(operation_tuple, tuple) or len(operation_tuple) == 0:
            raise ValueError("无效的操作符元组")

        operation_type = operation_tuple[0]

        if operation_type not in cls._strategies:
            raise ValueError(f"不支持的操作类型: {operation_type}")

        strategy_class = cls._strategies[operation_type]

        # 根据不同策略类型创建实例
        if operation_type in ("one", "list", "filter", "merge"):
            return strategy_class(operation_tuple[1])
        elif operation_type in ("left_join", "full_join"):
            return strategy_class(operation_tuple[1], operation_tuple[2])
        else:
            raise ValueError(f"未处理的操作类型: {operation_type}")
