from typing import Any, Callable
from .element import Element
from .utils import batch_process_data, get_join_condition, mapping, group_by
from antchain import element


class StrategyFactory:
    # 类属性：存储唯一实例（初始为 None）
    _instance = None

    def __init__(self):
        # 若需确保初始化逻辑只执行一次，可加判断
        if not hasattr(self, "_initialized"):
            # 你的初始化代码（如加载配置、创建资源等）
            self._initialized = True  # 标记已初始化
            self.processor: dict[str, Callable] = dict()
            self.processor["one"] = self.one
            self.processor["init"] = self.init
            self.processor["multi"] = self.multi
            self.processor["left_join"] = self.left_join
            self.processor["all_join"] = self.all_join
            self.processor["filter"] = self.filter
            self.processor["merge"] = self.merge

    # 重写 __new__ 方法，控制实例创建
    def __new__(cls, *args, **kwargs):
        # 如果实例不存在，创建并返回；否则直接返回已有实例
        if not cls._instance:
            cls._instance = super().__new__(cls, *args, **kwargs)
        return cls._instance

    def get_processor(self, element_type: str):
        return self.processor[element_type] if element_type in self.processor else None

    @staticmethod
    def process(element: Element) -> Any:
        if element is None:
            raise ValueError("element 不能为空")
        else:
            factory = StrategyFactory()
            processor = factory.get_processor(element.element_type)
            if processor is None:
                raise TypeError("不支持的element_type:" + element.element_type)
            left_data = element.left_data
            element.left_data = None
            return processor(element, left_data)

    def init(self, element: Element, left_data):
        if element.right_func is None:
            raise ValueError("right_func 不能为空")
        return element.right_func()

    def one(self, element: Element, left_data) -> Any:
        if element.right_func is None:
            raise ValueError("right_func 不能为空")
        if left_data is None:
            return [element.right_func(None)]
        if isinstance(left_data, list) or isinstance(left_data, tuple):
            return [element.right_func(item) for item in left_data]
        else:
            return [element.right_func(left_data)]

    def multi(self, element: Element, left_data) -> Any:
        if element.right_func is None:
            raise ValueError("right_func 不能为空")
        if left_data is None:
            return [element.right_func(None)]
        if isinstance(left_data, list) or isinstance(left_data, tuple):
            return batch_process_data(left_data, element.right_func)
        else:
            return [element.right_func(left_data)]

    def left_join(self, element: Element, left_data) -> Any:
        if element.join_func is None:
            raise ValueError("* 运算符的右边不能为空")
        # 拿到 * 后面的join条件信息
        left_key, right_key, left_property, one_to_many = self.__join_check(
            element, left_data
        )
        if left_data is None or len(left_data) == 0:
            return []
        # 批处理,拿到右侧数据
        right_data = batch_process_data(left_data, element.right_func)
        if right_data is None or len(right_data) == 0:
            return [left_data]
        # 连接左右两边的数据
        result = self.__left_join_merge(
            left_data,
            one_to_many,
            right_data,
            left_key,
            right_key,
            left_property,
        )
        return result

    def all_join(self, element: Element, left_data) -> Any:
        if element.join_func is None:
            raise ValueError("** 运算符的右边不能为空")
        left_key, right_key, left_property, one_to_many = self.__join_check(
            element, left_data
        )
        # 批处理,拿到右侧数据
        right_data = batch_process_data(left_data, element.right_func)
        if right_data is None or len(right_data) == 0:
            if left_data is None or len(left_data) == 0:
                return []
            else:
                return right_data
        # 连接左右两边的数据
        result = self.__left_join_merge(
            left_data,
            one_to_many,
            right_data,
            left_key,
            right_key,
            left_property,
        )
        right_data_dict = dict()
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
                if isinstance(item, list) or isinstance(item, tuple):
                    for i in item:
                        result.append(i)
                else:
                    result.append(item)
        return result

    def merge(self, element: Element, left_data):
        if element.right_func is None:
            raise ValueError("right_func 不能为空")
        result = list()
        data = element.right_func()
        if isinstance(data, list) or isinstance(data, tuple):
            result.extend(data)
        else:
            result.append(data)
        if left_data is None:
            return result
        elif isinstance(left_data, list) or isinstance(left_data, tuple):
            result = left_data + result  # pyright: ignore[reportOperatorIssue]
        else:
            result.insert(0, left_data)
        return result

    def filter(self, element: Element, left_data):
        if element.right_func is None:
            raise ValueError("right_func 不能为空")
        if left_data is None:
            return [element.right_func(None)]
        if isinstance(left_data, list) or isinstance(left_data, tuple):
            result = list()
            for data in left_data:
                if element.right_func(data):
                    result.append(data)
        return result

    def __join_check(self, element: Element, left_data):
        left_key, right_key, left_property, one_to_many = get_join_condition(
            element.join_func
        )
        if element.right_func is None:
            raise ValueError("& 运算符的右边不能为空")
        if left_key is None:
            raise ValueError("left_key 不能为空")
        if right_key is None:
            raise ValueError("right_key 不能为空")
        if one_to_many is None:
            raise ValueError("one_to_many 不能为空")
        return left_key, right_key, left_property, one_to_many

    def __left_join_merge(
        self,
        left_data: list | tuple,
        one_to_many: bool,
        right_data,
        left_key,
        right_key,
        left_property,
    ):
        # 如果有一边为空,那么都返回左边,因为是左连接
        if right_data is None:
            return [left_data]
        right_data_dict = dict()
        # 转换右边为字段,一对多转换为dict[key,list],一对一转换为dict[key,dict]
        if one_to_many:
            right_data_dict = group_by(right_data, right_key)
        else:
            right_data_dict = mapping(right_data, right_key)
        # 最终处理结果
        result = list()
        # 遍历左边
        for item in left_data:
            # 左边key
            l_k = left_key(item)
            # 根据左边key获取右边数据
            right_item = right_data_dict[l_k] if l_k in right_data_dict else None
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
                if isinstance(right_item, list) or isinstance(right_item, tuple):
                    for r_item in right_item:
                        result.append({**item, **r_item})
                else:
                    result.append({**item, **right_item})
        return result
