"""
Utils模块

该模块包含各种工具函数，用于支持数据流处理管道的核心功能。
包括函数参数检查、批处理、数据映射和分组等实用函数。
"""

import inspect
from typing import Callable, Any, Dict, List, Tuple, Union, Optional


def get_function_args_count(func: Callable[..., Any]) -> int:
    """
    根据函数签名获取函数的参数个数（不包括有默认值的参数）

    Args:
        func (Callable[..., Any]): 要检查的函数

    Returns:
        int: 函数参数的个数（无默认值的参数）
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
        func (Callable[..., Any]): 要检查的函数

    Returns:
        int: stream_size的值，如果拿不到返回0
    """
    sig = inspect.signature(func)
    stream_size = get_parameter_default_value(sig, "stream_size")
    return stream_size if stream_size else 0


def get_parameter_default_value(sig: inspect.Signature, parameter_name: str) -> Any:
    """
    获取函数上parameter_name的值

    Args:
        sig (Signature): 函数签名
        parameter_name (str): 参数名

    Returns:
        Any: parameter_name的值，如果拿不到返回None
    """
    if parameter_name in sig.parameters:
        param = sig.parameters[parameter_name]
        if param.default != inspect.Parameter.empty:
            return param.default
    return None


def get_join_condition(func: Callable[..., Any]) -> Tuple[Any, Any, Any, Any]:
    """
    获取函数上join_condition的值

    Args:
        func (Callable[..., Any]): 要检查的函数

    Returns:
        tuple: (left_key, right_key, left_property, one_to_many)的值，如果拿不到返回None
    """
    sig = inspect.signature(func)
    left_key = get_parameter_default_value(sig, "left_key")
    right_key = get_parameter_default_value(sig, "right_key")
    left_property = get_parameter_default_value(sig, "left_property")
    one_to_many = get_parameter_default_value(sig, "one_to_many")
    return (left_key, right_key, left_property, one_to_many)


def batch_process_data(
    data: Union[List[Any], Tuple[Any, ...]],
    func: Callable[..., Any],
    wrap_result: bool = True,
) -> List[Any]:
    """
    批处理数据

    Args:
        data (Union[List[Any], Tuple[Any, ...]]): 要处理的数据
        func (Callable[..., Any]): 处理函数
        wrap_result (bool): 是否将结果包装成列表，默认为True

    Returns:
        List[Any]: 处理结果
    """
    stream_size = get_stream_size(func)
    if stream_size <= 0:
        result_data = func(data)
        if isinstance(result_data, list):
            return result_data
        elif result_data is not None:
            return [result_data] if wrap_result else result_data
        else:
            return [] if wrap_result else []
    else:
        # 使用更高效的切片方式
        data_len = len(data)
        total_slices = (data_len + stream_size - 1) // stream_size
        result: List[Any] = list()
        # 循环处理每个切片
        for i in range(total_slices):
            # 计算当前切片的起始和结束索引
            start = i * stream_size
            # 优化结束索引计算，避免超出范围
            end = min(start + stream_size, data_len)
            slice_data = data[start:end]  # 截取切片
            # 当切片为空时直接跳出循环
            if len(slice_data) == 0:
                break
            func_data = func(slice_data)
            if func_data is None:
                continue
            elif isinstance(func_data, (list, tuple)):
                # 使用extend方法提高性能
                result.extend(func_data)
            else:
                result.append(func_data)
        return result


def get_function_return_type(func: Callable[..., Any]) -> Any:
    """
    获取函数的返回值类型

    Args:
        func (Callable[..., Any]): 要检查的函数

    Returns:
        Any: 函数的返回值类型，如果无法获取则返回None
    """
    sig = inspect.signature(func)
    return (
        sig.return_annotation
        if sig.return_annotation != inspect.Signature.empty
        else None
    )


def mapping(
    rows: List[Dict[str, Any]], key_func: Callable[[Dict[str, Any]], Any]
) -> Dict[Any, Dict[str, Any]]:
    """
    转换为key,value字典

    Args:
        rows (List[Dict[str, Any]]): 数据列表
        key_func (Callable[[Dict[str, Any]], Any]): 键函数

    Returns:
        Dict[Any, Dict[str, Any]]: 以key_func计算结果为键的字典
    """
    result: Dict[Any, Dict[str, Any]] = dict()
    for row in rows:
        result[key_func(row)] = row
    return result


def group_by(
    rows: List[Dict[str, Any]], key_func: Callable[[Dict[str, Any]], Any]
) -> Dict[Any, List[Dict[str, Any]]]:
    """
    按指定的key分组

    Args:
        rows (List[Dict[str, Any]]): 数据列表
        key_func (Callable[[Dict[str, Any]], Any]): 分组键函数

    Returns:
        Dict[Any, List[Dict[str, Any]]]: 以key_func计算结果为键的分组字典
    """
    result: Dict[Any, List[Dict[str, Any]]] = dict()
    for row in rows:
        k = key_func(row)
        # 使用更高效的键存在性检查
        if k in result:
            result[k].append(row)
        else:
            # 直接创建列表并添加元素
            result[k] = [row]
    return result
