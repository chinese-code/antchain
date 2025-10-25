"""
Validators模块

该模块包含各种验证函数，用于验证数据流处理管道中的参数和条件。
"""

from typing import Any, Callable


def validate_function_args_count(
    func: Callable[..., Any], expected_count: int, operation_name: str
) -> None:
    """
    验证函数参数个数

    Args:
        func (Callable[..., Any]): 要验证的函数
        expected_count (int): 期望的参数个数
        operation_name (str): 操作名称，用于错误信息

    Raises:
        ValidationError: 当函数参数个数不符合期望时
    """
    import inspect

    sig = inspect.signature(func)
    count = 0
    for param in sig.parameters.values():
        if param.default == inspect.Parameter.empty:
            count += 1

    if count != expected_count:
        from .exceptions import ValidationError

        raise ValidationError(
            f"{operation_name}函数 {func.__name__} 必须且只能有{expected_count}个参数，"
            + f"当前参数个数: {count}"
        )


def validate_filter_function(func: Callable[..., Any]) -> None:
    """
    验证过滤函数

    Args:
        func (Callable[..., Any]): 要验证的过滤函数

    Raises:
        ValidationError: 当函数参数或返回值类型不符合要求时
    """
    from .utils import get_function_args_count, get_function_return_type

    args_count = get_function_args_count(func)
    if args_count > 1:
        from .exceptions import ValidationError

        raise ValidationError(
            f"过滤函数 {func.__name__} 最多只能有一个参数，当前参数个数: {args_count}"
        )

    return_type = get_function_return_type(func)
    if return_type != bool and return_type is not None:
        from .exceptions import ValidationError

        raise ValidationError(
            f"过滤函数 {func.__name__} 的返回值类型必须是bool，当前类型: {return_type}"
        )


def validate_join_conditions(
    element: Any, left_key: Any, right_key: Any, one_to_many: Any
) -> None:
    """
    验证连接条件

    Args:
        element (Any): Element实例
        left_key (Any): 左键
        right_key (Any): 右键
        one_to_many (Any): 一对多标志

    Raises:
        JoinError: 当连接条件不满足时
    """
    from .exceptions import JoinError

    if element.right_func is None:
        raise JoinError("& 运算符的右边不能为空")
    if left_key is None:
        raise JoinError("left_key 不能为空")
    if right_key is None:
        raise JoinError("right_key 不能为空")
    if one_to_many is None:
        raise JoinError("one_to_many 不能为空")
