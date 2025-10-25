import unittest
from antchain.element import Element
from antchain.strategy import StrategyFactory
from antchain.stream import Stream, Start
from antchain.exceptions import (
    ValidationError,
    StrategyError,
    ProcessingError,
    JoinError,
)


class TestExceptions(unittest.TestCase):

    def test_validation_error_in_sub_operator(self):
        """测试-操作符中的验证异常"""

        def invalid_filter_func(item1, item2):
            return True

        element = Element()
        with self.assertRaises(ValidationError) as context:
            element - invalid_filter_func

        self.assertIn("过滤函数", str(context.exception))
        self.assertIn("最多只能有一个参数", str(context.exception))

    def test_validation_error_in_gt_operator(self):
        """测试>操作符中的验证异常"""

        def invalid_process_func(item1, item2):
            return item1

        element = Element()
        with self.assertRaises(ValidationError) as context:
            element > invalid_process_func

        self.assertIn("单条处理函数", str(context.exception))
        self.assertIn("必须且只能有1个参数", str(context.exception))

    def test_validation_error_in_rshift_operator(self):
        """测试>>操作符中的验证异常"""

        def invalid_process_func(item1, item2):
            return [item1]

        element = Element()
        with self.assertRaises(ValidationError) as context:
            element >> invalid_process_func

        self.assertIn("批处理函数", str(context.exception))
        self.assertIn("必须且只能有1个参数", str(context.exception))

    def test_strategy_error_invalid_element_type(self):
        """测试无效元素类型的策略异常"""
        element = Element(element_type="invalid")
        with self.assertRaises(StrategyError) as context:
            StrategyFactory.process(element)

        self.assertIn("不支持的element_type", str(context.exception))

    def test_processing_error_in_init_processor(self):
        """测试初始化处理器中的处理异常"""

        def failing_init_func():
            raise ValueError("初始化失败")

        element = Element(element_type="init", right_func=failing_init_func)
        with self.assertRaises(ProcessingError) as context:
            StrategyFactory.process(element)

        self.assertIn("初始化函数执行失败", str(context.exception))
        self.assertIn("初始化失败", str(context.exception))

    def test_processing_error_in_one_processor(self):
        """测试单条处理器中的处理异常"""

        def failing_process_func(item):
            raise ValueError("处理失败")

        element = Element(element_type="one", right_func=failing_process_func)
        with self.assertRaises(ProcessingError) as context:
            factory = StrategyFactory()
            factory.one(element, [{"id": 1}])

        self.assertIn("单条处理函数执行失败", str(context.exception))
        self.assertIn("处理失败", str(context.exception))

    def test_join_error_in_left_join(self):
        """测试左连接中的连接异常"""

        def failing_join_func():
            raise ValueError("连接失败")

        # 创建一个有效的element用于测试
        def dummy_right_func(rows):
            return []

        element = Element(
            element_type="left_join",
            join_func=failing_join_func,
            right_func=dummy_right_func,
        )
        with self.assertRaises(JoinError) as context:
            factory = StrategyFactory()
            factory.left_join(element, [{"id": 1}])

        self.assertIn("左连接操作失败", str(context.exception))


if __name__ == "__main__":
    unittest.main()
