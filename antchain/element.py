from typing import Callable, Any
from .utils import get_function_args_count, get_function_return_type


class Element:

    def __init__(
        self,
        element_type: str = "none",
        left_data=None,
        right_func: Callable | None = None,
        join_func: Callable | None = None,
    ):
        self.left_data: None | list | tuple | Any = None
        self.element_type = element_type
        self.right_func = right_func
        self.join_func = join_func

    def __and__(self, other: Callable):
        return Element(element_type="and", right_func=other)

    def __add__(self, other: Callable):
        return Element(element_type="merge", right_func=other)

    def __sub__(self, other):
        args_count = get_function_args_count(other)
        if args_count > 1:
            raise ValueError("> %s,函数最多只能有一个参数".format(other.__name__))
        return_type = get_function_return_type(other)
        if return_type != bool and return_type != None:
            raise ValueError("> %s,函数的返回值类型必须是bool".format(other.__name__))
        return Element(element_type="filter", right_func=other)

    def __mul__(self, other):
        self.element_type = "left_join"
        self.join_func = other
        return self

    def __pow__(self, other):
        self.element_type = "all_join"
        self.join_func = other
        return self

    def __gt__(self, other):
        args_count = get_function_args_count(other)
        if args_count != 1:
            raise ValueError("> %s,函数最多只能有一个参数".format(other.__name__))
        return Element(element_type="one", right_func=other)

    def __rshift__(self, other):
        args_count = get_function_args_count(other)
        if args_count != 1:
            raise ValueError(">> %s 函数最多只能有一个参数".format(other.__name__))
        return Element(element_type="multi", right_func=other)
