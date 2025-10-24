from typing import Any, Callable, List, Tuple, Set, Dict
from abc import ABC, abstractmethod
import inspect
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


class JoinStrategy(ProcessingStrategy, ABC):
    """连接策略抽象基类"""

    def __init__(self, condition_func: Callable, data_func: Callable):
        self.condition_func = condition_func
        self.data_func = data_func

    def process(self, prev_result: Any, *args, **kwargs) -> Any:
        """连接处理的通用流程"""
        # 检查是否是单函数模式
        if hasattr(self.condition_func, "_extract_func"):
            # 处理单函数模式
            condition_func, right_data = self._process_single_function_mode(prev_result)
        else:
            # 处理传统模式
            condition_func = self.condition_func
            right_data = self._process_traditional_mode(prev_result)

        # 确保左右数据都是列表
        left_data = prev_result if isinstance(prev_result, list) else [prev_result]
        right_data = right_data if isinstance(right_data, list) else [right_data]

        # 执行具体的连接操作
        return self._perform_join(left_data, right_data, condition_func)

    def _process_single_function_mode(self, prev_result: Any) -> Tuple[Callable, Any]:
        """处理单函数模式"""
        # 从单函数中提取条件函数和数据函数信息，传递prev_result参数
        extract_func = self.condition_func._extract_func
        condition_func, right_data_info = extract_func(prev_result)

        # 如果right_data_info不是元组，说明数据已经被处理过了，直接返回
        if not (isinstance(right_data_info, tuple) and len(right_data_info) == 3):
            # 直接使用提取的数据
            return condition_func, right_data_info

        # 提取函数、签名和prev_result信息
        real_data_func, sig, extract_prev_result = right_data_info

        # 如果real_data_func不可调用，直接返回
        if not callable(real_data_func):
            return condition_func, real_data_func

        # 检查是否有stream_size参数
        batch_size = extract_batch_size(real_data_func, 0)
        if (
            batch_size > 0
            and isinstance(prev_result, list)
            and len(prev_result) > batch_size
        ):
            # 使用batch_process函数分批处理
            right_data = batch_process(prev_result, batch_size, real_data_func)
            return condition_func, right_data

        # 检查函数是否可以接受参数
        has_required_params = any(
            param.default == inspect.Parameter.empty
            for param in sig.parameters.values()
            if param.name not in ["stream_size", "stream_join"]
        )

        # 检查函数是否可以接受参数
        if len(sig.parameters) > 0 and has_required_params and prev_result is not None:
            right_data = real_data_func(prev_result)
        else:
            # 函数不接受任何参数
            right_data = real_data_func()

        return condition_func, right_data

    def _process_traditional_mode(self, prev_result: Any) -> Any:
        """处理传统模式"""
        # 如果data_func不可调用，直接返回
        if not callable(self.data_func):
            return self.data_func

        # 检查data_func是否可以接受参数
        sig = inspect.signature(self.data_func)
        if len(sig.parameters) == 0:
            return self.data_func()

        # 检查是否有stream_size参数
        batch_size = extract_batch_size(self.data_func, 0)
        if (
            batch_size > 0
            and isinstance(prev_result, list)
            and len(prev_result) > batch_size
        ):
            # 使用batch_process函数分批处理
            return batch_process(prev_result, batch_size, self.data_func)

        # 检查函数是否可以接受参数
        has_required_params = any(
            param.default == inspect.Parameter.empty
            for param in sig.parameters.values()
            if param.name not in ["stream_size", "stream_join"]
        )

        # 检查函数是否可以接受参数
        if len(sig.parameters) > 0 and has_required_params and prev_result is not None:
            return self.data_func(prev_result)
        else:
            # 函数不接受任何参数
            return self.data_func()

    @abstractmethod
    def _perform_join(
        self, left_data: List[Any], right_data: List[Any], condition_func: Callable
    ) -> List[Any]:
        """执行具体的连接操作"""
        pass


class LeftJoinStrategy(JoinStrategy):
    """左连接数据处理策略 (*)"""

    def _perform_join(
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


class FullJoinStrategy(JoinStrategy):
    """全连接数据处理策略 (**)"""

    def _perform_join(
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
