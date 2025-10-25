"""
异常模块

该模块定义了AntChain库中使用的所有自定义异常类型。
通过继承关系组织异常类型，便于异常处理和调试。
"""


class AntChainError(Exception):
    """
    AntChain库的基础异常类

    所有AntChain库中定义的异常都应该继承自此类。
    """

    pass


class ElementError(AntChainError):
    """
    Element相关的异常

    当Element类的操作出现错误时抛出此异常。
    """

    pass


class StrategyError(AntChainError):
    """
    Strategy相关的异常

    当策略处理过程中出现错误时抛出此异常。
    """

    pass


class ProcessingError(AntChainError):
    """
    数据处理过程中的异常

    当数据流处理过程中出现错误时抛出此异常。
    """

    pass


class ValidationError(AntChainError):
    """
    参数验证相关的异常

    当函数参数或返回值验证失败时抛出此异常。
    """

    pass


class JoinError(AntChainError):
    """
    连接操作相关的异常

    当连接操作（左连接、全连接等）出现错误时抛出此异常。
    """

    pass


class BatchProcessError(AntChainError):
    """
    批处理相关的异常

    当批处理操作出现错误时抛出此异常。
    """

    pass
