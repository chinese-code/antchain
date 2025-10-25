from typing import Any, Callable
from .element import Element
from .utils import batch_process_data


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
            return processor(element)

    def init(self, element: Element):
        if element.right_func is None:
            raise ValueError("right_func 不能为空")
        return element.right_func()

    def one(self, element: Element) -> Any:
        if element.right_func is None:
            raise ValueError("right_func 不能为空")
        if element.left_data is None:
            return [element.right_func(None)]
        if isinstance(element.left_data, list) or isinstance(element.left_data, tuple):
            return [element.right_func(item) for item in element.left_data]
        else:
            return [element.right_func(element.left_data)]

    def multi(self, element: Element) -> Any:
        if element.right_func is None:
            raise ValueError("right_func 不能为空")
        if element.left_data is None:
            return [element.right_func(None)]
        if isinstance(element.left_data, list) or isinstance(element.left_data, tuple):
            return batch_process_data(element.left_data, element.right_func)
        else:
            return [element.right_func(element.left_data)]

    def left_join(self, element: Element) -> Any:
        pass

    def all_join(self, element: Element) -> Any:
        pass

    def merge(self, element: Element):
        pass

    def filter(self, element: Element):
        if element.right_func is None:
            raise ValueError("right_func 不能为空")
        if element.left_data is None:
            return [element.right_func(None)]
        if isinstance(element.left_data, list) or isinstance(element.left_data, tuple):
            result = list()
            for data in element.left_data:
                if element.right_func(data):
                    result.append(data)
        return result
