import inspect
from typing import Callable, Any


def get_function_args_count(func: Callable[..., Any]) -> int:
    """
    根据函数签名获取函数的参数个数

    Args:
        func: 要检查的函数

    Returns:
        int: 函数参数的个数
    """
    sig = inspect.signature(func)
    # 统计所有默认值为 empty 的参数（即无默认值的参数）
    count = 0
    for param in sig.parameters.values():
        if param.default == inspect.Parameter.empty:
            count += 1
    return count


def get_stream_size(func: Callable[..., Any]) -> int:
    """
    获取函数上stream_size的值

    Args:
        func: 要检查的函数

    Returns:
        int: stream_size的值，如果拿不到返回0
    """
    sig = inspect.signature(func)
    if "stream_size" in sig.parameters:
        param = sig.parameters["stream_size"]
        if param.default != inspect.Parameter.empty:
            return param.default
    return 0


def batch_process_data(data: list | tuple, func: Callable):
    stream_size = get_stream_size(func)
    if stream_size <= 0:
        return func(data)
    else:
        total_slices = (len(data) + stream_size - 1) // stream_size
        result = list()
        # 循环处理每个切片
        for i in range(total_slices):
            # 计算当前切片的起始和结束索引
            start = i * stream_size
            end = start + stream_size
            slice_data = data[start:end]  # 截取切片
            if len(slice_data) == 0:
                break
            result.extend(func(slice_data))
        return result
