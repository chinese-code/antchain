"""
数据流处理工具模块
"""

import inspect
from typing import Any, Callable, List


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
        pass

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
