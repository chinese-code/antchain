"""
数据流处理工具模块
"""

import inspect
from typing import Any, Callable, List, Tuple
from typing import cast


def extract_batch_size(func: Callable, default_size: int = 0) -> int:
    """
    从函数的默认参数中提取批处理大小

    Args:
        func: 处理函数
        default_size: 默认批处理大小，0表示全量处理

    Returns:
        批处理大小，如果无法提取或无效则返回默认值
    """
    if not callable(func):
        return default_size

    try:
        # 获取函数的签名
        sig = inspect.signature(func)
        # 查找stream_size参数
        if "stream_size" in sig.parameters:
            param = sig.parameters["stream_size"]
            # 如果有默认值且是正数，则使用该值
            if (
                param.default != inspect.Parameter.empty
                and isinstance(param.default, int)
                and param.default > 0
            ):
                return param.default
    except (ValueError, TypeError):
        # 让异常传播，而不是静默忽略
        raise

    return default_size


def batch_process(
    data: List[Any], batch_size: int, process_func: Callable, *args, **kwargs
) -> List[Any]:
    """
    批处理数据

    Args:
        data: 要处理的数据列表
        *args: 传递给处理函数的位置参数
        **kwargs: 传递给处理函数的关键字参数

    Returns:
        处理后的结果列表
    """
    if batch_size <= 0:
        # 全量处理
        return process_func(data, *args, **kwargs)

    # 分批处理
    result = []
    for i in range(0, len(data), batch_size):
        batch = data[i : i + batch_size]
        batch_result = process_func(batch, *args, **kwargs)
        if isinstance(batch_result, list):
            result.extend(batch_result)
        else:
            result.append(batch_result)

    return result


def create_single_function_wrappers(other: Callable) -> Tuple[Callable, Callable]:
    """
    创建单函数模式的包装器函数

    Args:
        other: 原始函数

    Returns:
        包含条件函数包装器和数据函数包装器的元组
    """

    def extract_condition_data(prev_result=None):
        # 获取函数的stream_join参数
        condition_func = None
        try:
            sig = inspect.signature(other)
            if "stream_join" in sig.parameters:
                param = sig.parameters["stream_join"]
                if param.default != inspect.Parameter.empty and callable(param.default):
                    condition_func = param.default
        except (ValueError, TypeError):
            # 让异常传播，而不是静默忽略
            raise

        # 执行函数获取数据，如果函数可以接受参数则传递prev_result
        try:
            # 检查是否有非默认参数
            has_required_params = any(
                param.default == inspect.Parameter.empty
                for param in sig.parameters.values()
                if param.name not in ["stream_size", "stream_join"]
            )
            # 检查函数是否可以接受参数
            if (
                len(sig.parameters) > 0
                and has_required_params
                and prev_result is not None
            ):
                data = other(prev_result)
            else:
                # 函数不接受任何参数
                data = other()
        except (TypeError, ValueError):
            # 让异常传播，而不是静默忽略
            raise
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
    # 使用 cast 来告诉类型检查器这些函数有额外的属性
    condition_func_wrapper = cast(Callable, condition_func_wrapper)
    data_func_wrapper = cast(Callable, data_func_wrapper)
    condition_func_wrapper._extract_func = extract_condition_data  # type: ignore
    data_func_wrapper._extract_func = lambda: extract_condition_data(None)  # type: ignore

    return condition_func_wrapper, data_func_wrapper
