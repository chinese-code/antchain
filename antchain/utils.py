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
    stream_size = get_parameter_default_value(sig, "stream_size")
    return stream_size if stream_size else 0


def get_parameter_default_value(sig, parameter_name):
    """
    获取函数上parameter_name的值

    Args:
        func: 要检查的函数
        parameter_name: 参数名

    Returns:
        int: parameter_name的值，如果拿不到返回None
    """
    if parameter_name in sig.parameters:
        param = sig.parameters[parameter_name]
        if param.default != inspect.Parameter.empty:
            return param.default
    return None


def get_join_condition(func):
    """
    获取函数上join_condition的值

    Args:
        func: 要检查的函数

    Returns:
        tuple: join_condition的值，如果拿不到返回None
    """
    sig = inspect.signature(func)
    left_key = get_parameter_default_value(sig, "left_key")
    right_key = get_parameter_default_value(sig, "right_key")
    left_property = get_parameter_default_value(sig, "left_property")
    one_to_many = get_parameter_default_value(sig, "one_to_many")
    return (left_key, right_key, left_property, one_to_many)


def batch_process_data(data: list | tuple, func):
    stream_size = get_stream_size(func)
    if stream_size <= 0:
        return func(data)
    else:
        total_slices = (len(data) + stream_size - 1)
        result = list()
        # 循环处理每个切片
        for i in range(total_slices):
            # 计算当前切片的起始和结束索引
            start = i * stream_size
            end = start + stream_size
            slice_data = data[start:end]  # 截取切片
            if len(slice_data) == 0:
                break
            func_data = func(slice_data)
            if func_data is None:
                continue
            elif isinstance(func_data, list) or isinstance(func_data, tuple):
                result.extend(func_data)
            else:
                result.append(func_data)
        return result


def get_function_return_type(func: Callable[..., Any]) -> Any:
    """
    获取函数的返回值类型

    Args:
        func: 要检查的函数

    Returns:
        Any: 函数的返回值类型，如果无法获取则返回None
    """
    sig = inspect.signature(func)
    return (
        sig.return_annotation
        if sig.return_annotation != inspect.Signature.empty
        else None
    )


def mapping(rows, key_func):
    """
    转换为key,value
    :param rows:
    :param key_func:
    :return:
    """
    result = dict()
    for row in rows:
        result[key_func(row)] = row
    return result


def group_by(rows, key_func):
    """
    按指定的key分组
    :param rows: 数据列表
    :param key_func:  分组key
    :return:
    """
    result = dict()
    for row in rows:
        k = key_func(row)
        if k in result.keys():
            result[k].append(row)
        else:
            t = list()
            t.append(row)
            result[k] = t
    return result
