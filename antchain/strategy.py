"""
Strategy模块

该模块实现了数据流处理的各种策略，包括单条处理、批处理、过滤、合并、连接等操作。
StrategyFactory采用单例模式，确保整个应用中只有一个策略工厂实例。

策略类型：
- init: 初始化策略
- one: 单条处理策略
- multi: 批处理策略
- left_join: 左连接策略
- all_join: 全连接策略
- filter: 过滤策略
- merge: 合并策略
"""

from typing import Any, Callable, Dict, List, Tuple, Union, Optional
from .element import Element
from .utils import batch_process_data, get_join_condition, mapping, group_by
from .validators import validate_join_conditions
from .exceptions import StrategyError, ProcessingError, JoinError


class StrategyFactory:
    """
    策略工厂类，采用单例模式

    该类负责根据Element的类型选择相应的处理策略，并执行具体的处理逻辑。
    使用单例模式确保全局只有一个策略工厂实例。
    """

    # 类属性：存储唯一实例（初始为 None）
    _instance: Optional["StrategyFactory"] = None

    def __init__(self) -> None:
        """
        初始化策略工厂

        初始化时会创建各种处理策略的映射表。
        """
        # 若需确保初始化逻辑只执行一次，可加判断
        if not hasattr(self, "_initialized"):
            # 你的初始化代码（如加载配置、创建资源等）
            self._initialized = True  # 标记已初始化
            self.processor: Dict[str, Callable[..., Any]] = dict()
            self.processor["one"] = self.one
            self.processor["init"] = self.init
            self.processor["multi"] = self.multi
            self.processor["left_join"] = self.left_join
            self.processor["all_join"] = self.all_join
            self.processor["filter"] = self.filter
            self.processor["merge"] = self.merge

    # 重写 __new__ 方法，控制实例创建
    def __new__(cls, *args: Any, **kwargs: Any) -> "StrategyFactory":
        """
        创建策略工厂实例，确保单例模式

        Returns:
            StrategyFactory: 策略工厂实例
        """
        # 如果实例不存在，创建并返回；否则直接返回已有实例
        if not cls._instance:
            cls._instance = super().__new__(cls, *args, **kwargs)
        return cls._instance

    def get_processor(self, element_type: str) -> Union[Callable[..., Any], None]:
        """
        获取指定类型的处理器

        Args:
            element_type (str): 元素类型

        Returns:
            Callable | None: 对应的处理器函数，如果不存在则返回None
        """
        return self.processor[element_type] if element_type in self.processor else None

    @staticmethod
    def process(element: Element) -> Any:
        """
        处理Element元素

        Args:
            element (Element): 要处理的元素

        Returns:
            Any: 处理结果

        Raises:
            StrategyError: 当element为空或不支持的element_type时
            ProcessingError: 当处理过程中出现异常时
        """
        if element is None:
            raise StrategyError("element 不能为空")
        else:
            factory = StrategyFactory()
            processor = factory.get_processor(element.element_type)
            if processor is None:
                raise StrategyError("不支持的element_type:" + element.element_type)
            left_data = element.left_data
            element.left_data = None
            return processor(element, left_data)

    def init(self, element: Element, left_data: Any) -> Any:
        """
        初始化策略

        Args:
            element (Element): 元素
            left_data (Any): 左侧数据

        Returns:
            Any: 初始化函数的返回值

        Raises:
            StrategyError: 当right_func为空时
            ProcessingError: 当初始化函数执行失败时
        """
        if element.right_func is None:
            raise StrategyError("right_func 不能为空")
        try:
            return element.right_func()
        except Exception as e:
            raise ProcessingError(f"初始化函数执行失败: {str(e)}") from e

    def one(self, element: Element, left_data: Any) -> List[Any]:
        """
        单条处理策略

        Args:
            element (Element): 元素
            left_data (Any): 左侧数据

        Returns:
            List[Any]: 处理结果列表

        Raises:
            StrategyError: 当right_func为空时
            ProcessingError: 当处理函数执行失败时
        """
        if element.right_func is None:
            raise StrategyError("right_func 不能为空")
        try:
            if left_data is None:
                return [element.right_func(None)]
            if isinstance(left_data, list) or isinstance(left_data, tuple):
                return [element.right_func(item) for item in left_data]
            else:
                return [element.right_func(left_data)]
        except Exception as e:
            raise ProcessingError(f"单条处理函数执行失败: {str(e)}") from e

    def multi(self, element: Element, left_data: Any) -> Any:
        """
        批处理策略

        Args:
            element (Element): 元素
            left_data (Any): 左侧数据

        Returns:
            Any: 处理结果

        Raises:
            StrategyError: 当right_func为空时
            ProcessingError: 当批处理函数执行失败时
        """
        if element.right_func is None:
            raise StrategyError("right_func 不能为空")
        try:
            if left_data is None:
                return element.right_func(None)
            if isinstance(left_data, list) or isinstance(left_data, tuple):
                return batch_process_data(left_data, element.right_func, wrap_result=False)  # type: ignore
            else:
                return element.right_func(left_data)
        except Exception as e:
            raise ProcessingError(f"批处理函数执行失败: {str(e)}") from e

    def left_join(self, element: Element, left_data: Any) -> List[Any]:
        """
        左连接策略

        Args:
            element (Element): 元素
            left_data (Any): 左侧数据

        Returns:
            List[Any]: 连接结果

        Raises:
            JoinError: 当连接函数或条件不满足时
        """
        if element.join_func is None:
            raise JoinError("* 运算符的右边不能为空")
        try:
            # 拿到 * 后面的join条件信息
            left_key, right_key, left_property, one_to_many = self._join_check(
                element, left_data
            )
            if left_data is None or len(left_data) == 0:
                return []
            r_func = element.right_func
            # 批处理,拿到右侧数据
            right_data = batch_process_data(left_data, r_func)  # type: ignore
            if right_data is None or len(right_data) == 0:
                return list(left_data) if isinstance(left_data, tuple) else left_data
            # 连接左右两边的数据
            result = self._left_join_merge(
                left_data,
                one_to_many,
                right_data,
                left_key,
                right_key,
                left_property,
            )
            return result
        except Exception as e:
            raise JoinError(f"左连接操作失败: {str(e)}") from e

    def all_join(self, element: Element, left_data: Any) -> List[Any]:
        """
        全连接策略

        Args:
            element (Element): 元素
            left_data (Any): 左侧数据

        Returns:
            List[Any]: 连接结果

        Raises:
            JoinError: 当连接函数或条件不满足时
        """
        if element.join_func is None:
            raise JoinError("** 运算符的右边不能为空")
        try:
            left_key, right_key, left_property, one_to_many = self._join_check(
                element, left_data
            )
            r_func = element.right_func
            # 批处理,拿到右侧数据
            right_data = batch_process_data(left_data, r_func)  # type: ignore
            if right_data is None or len(right_data) == 0:
                if left_data is None or len(left_data) == 0:
                    return []
                else:
                    return (
                        list(left_data) if isinstance(left_data, tuple) else left_data
                    )
            # 连接左右两边的数据
            result = self._left_join_merge(
                left_data,
                one_to_many,
                right_data,
                left_key,
                right_key,
                left_property,
            )
            right_data_dict: Dict[Any, Any] = dict()
            # 转换右边为字段,一对多转换为dict[key,list],一对一转换为dict[key,dict]
            if one_to_many:
                right_data_dict = group_by(right_data, right_key)
            else:
                right_data_dict = mapping(right_data, right_key)
            # 转换左边为dict
            left_data_dict = mapping(left_data, left_key)
            # 循环右边
            for r_k, item in right_data_dict.items():
                # 不在左边的数据,需要插入到结果中,并且这部分数据不用管left_property
                if r_k not in left_data_dict:
                    if isinstance(item, list):
                        for i in item:
                            result.append(i)
                    elif isinstance(item, tuple):
                        result.extend(list(item))
                    else:
                        result.append(item)
            return result
        except Exception as e:
            raise JoinError(f"全连接操作失败: {str(e)}") from e

    def merge(self, element: Element, left_data: Any) -> List[Any]:
        """
        合并策略

        Args:
            element (Element): 元素
            left_data (Any): 左侧数据

        Returns:
            List[Any]: 合并结果

        Raises:
            StrategyError: 当right_func为空时
            ProcessingError: 当合并函数执行失败时
        """
        if element.right_func is None:
            raise StrategyError("right_func 不能为空")
        try:
            result: List[Any] = list()
            data = element.right_func()
            if isinstance(data, list) or isinstance(data, tuple):
                result.extend(data)
            else:
                result.append(data)
            if left_data is None:
                return result
            elif isinstance(left_data, list) or isinstance(left_data, tuple):
                # 将左侧数据转换为列表进行合并
                left_list = (
                    list(left_data) if isinstance(left_data, tuple) else left_data
                )
                result = left_list + result
            else:
                result.insert(0, left_data)
            return result
        except Exception as e:
            raise ProcessingError(f"合并操作失败: {str(e)}") from e

    def filter(self, element: Element, left_data: Any) -> List[Any]:
        """
        过滤策略

        Args:
            element (Element): 元素
            left_data (Any): 左侧数据

        Returns:
            List[Any]: 过滤结果

        Raises:
            StrategyError: 当right_func为空时
            ProcessingError: 当过滤函数执行失败时
        """
        if element.right_func is None:
            raise StrategyError("right_func 不能为空")
        try:
            if left_data is None:
                return []
            if isinstance(left_data, list) or isinstance(left_data, tuple):
                result: List[Any] = list()
                for data in left_data:
                    if element.right_func(data):
                        result.append(data)
                return result
            return []
        except Exception as e:
            raise ProcessingError(f"过滤操作失败: {str(e)}") from e

    def _join_check(
        self, element: Element, left_data: Any
    ) -> Tuple[Any, Any, Any, Any]:
        """
        连接条件检查

        Args:
            element (Element): 元素
            left_data (Any): 左侧数据

        Returns:
            tuple: (left_key, right_key, left_property, one_to_many)

        Raises:
            JoinError: 当连接条件不满足时
        """
        if element.join_func is not None:
            left_key, right_key, left_property, one_to_many = get_join_condition(
                element.join_func
            )
            validate_join_conditions(element, left_key, right_key, one_to_many)
            return left_key, right_key, left_property, one_to_many
        else:
            from .exceptions import JoinError

            raise JoinError("join_func 不能为空")

    def _left_join_merge(
        self,
        left_data: Union[List[Any], Tuple[Any, ...]],
        one_to_many: bool,
        right_data: Any,
        left_key: Callable[..., Any],
        right_key: Callable[..., Any],
        left_property: Optional[str],
    ) -> List[Any]:
        """
        左连接合并操作

        Args:
            left_data (Union[List[Any], Tuple[Any, ...]]): 左侧数据
            one_to_many (bool): 是否一对多连接
            right_data (Any): 右侧数据
            left_key (Callable): 左侧键函数
            right_key (Callable): 右侧键函数
            left_property (Optional[str]): 左侧属性名

        Returns:
            List[Any]: 合并结果
        """
        # 如果有一边为空,那么都返回左边,因为是左连接
        if right_data is None or len(right_data) == 0:
            return list(left_data) if isinstance(left_data, tuple) else left_data
        right_data_dict: Dict[Any, Any] = dict()
        # 转换右边为字段,一对多转换为dict[key,list],一对一转换为dict[key,dict]
        if one_to_many:
            right_data_dict = group_by(right_data, right_key)
        else:
            right_data_dict = mapping(right_data, right_key)
        # 最终处理结果
        result: List[Any] = list()
        # 遍历左边
        for item in left_data:
            try:
                # 左边key
                l_k = left_key(item)
                # 根据左边key获取右边数据，使用更高效的get方法
                right_item = right_data_dict.get(l_k)
                # 右边数据为空,那么只把左边数据加入到结果集中
                if right_item is None:
                    result.append(item)
                    continue
                # 有left_property时,那么把右边数据加入到左边的一个属性字段中
                if left_property is not None:
                    item[left_property] = right_item
                    result.append(item)
                else:
                    # 没有left_property时,那么右边有多少条,就生成左边的数据.保证数据对齐.
                    if isinstance(right_item, list):
                        for r_item in right_item:
                            result.append({**item, **r_item})
                    elif isinstance(right_item, tuple):
                        result.append({**item, **dict(right_item)})
                    else:
                        result.append({**item, **right_item})
            except Exception as e:
                raise JoinError(f"连接合并过程中出现错误: {str(e)}") from e
        return result
