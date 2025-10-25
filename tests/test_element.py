import unittest
from antchain.element import Element


class TestElement(unittest.TestCase):

    def test_element_initialization(self):
        """测试Element初始化"""
        element = Element()
        self.assertEqual(element.element_type, "none")
        self.assertIsNone(element.left_data)
        self.assertIsNone(element.right_func)
        self.assertIsNone(element.join_func)

    def test_element_and_operator(self):
        """测试&操作符"""

        def dummy_func():
            return []

        element = Element()
        result = element & dummy_func
        self.assertEqual(result.element_type, "and")
        self.assertEqual(result.right_func, dummy_func)

    def test_element_add_operator(self):
        """测试+操作符"""

        def dummy_func():
            return []

        element = Element()
        result = element + dummy_func
        self.assertEqual(result.element_type, "merge")
        self.assertEqual(result.right_func, dummy_func)

    def test_element_sub_operator(self):
        """测试-操作符"""

        def filter_func(item):
            return True

        element = Element()
        result = element - filter_func
        self.assertEqual(result.element_type, "filter")
        self.assertEqual(result.right_func, filter_func)

    def test_element_mul_operator(self):
        """测试*操作符"""

        def join_func():
            pass

        element = Element()
        result = element * join_func
        self.assertEqual(result.element_type, "left_join")
        self.assertEqual(result.join_func, join_func)

    def test_element_pow_operator(self):
        """测试**操作符"""

        def join_func():
            pass

        element = Element()
        result = element**join_func
        self.assertEqual(result.element_type, "all_join")
        self.assertEqual(result.join_func, join_func)

    def test_element_gt_operator(self):
        """测试>操作符"""

        def process_func(item):
            return item

        element = Element()
        result = element > process_func
        self.assertEqual(result.element_type, "one")
        self.assertEqual(result.right_func, process_func)

    def test_element_rshift_operator(self):
        """测试>>操作符"""

        def process_func(items):
            return items

        element = Element()
        result = element >> process_func
        self.assertEqual(result.element_type, "multi")
        self.assertEqual(result.right_func, process_func)

    def test_element_sub_operator_validation(self):
        """测试-操作符参数验证"""

        def invalid_filter_func(item, extra_param):
            return True

        element = Element()
        with self.assertRaises(Exception) as context:
            element - invalid_filter_func

        self.assertIn("过滤函数", str(context.exception))
        self.assertIn("最多只能有一个参数", str(context.exception))

    def test_element_gt_operator_validation(self):
        """测试>操作符参数验证"""

        def invalid_process_func(item1, item2):
            return item1

        element = Element()
        with self.assertRaises(Exception) as context:
            element > invalid_process_func

        self.assertIn("单条处理函数", str(context.exception))
        self.assertIn("必须且只能有1个参数", str(context.exception))

    def test_element_rshift_operator_validation(self):
        """测试>>操作符参数验证"""

        def invalid_process_func(item1, item2):
            return item1

        element = Element()
        with self.assertRaises(Exception) as context:
            element >> invalid_process_func

        self.assertIn("批处理函数", str(context.exception))
        self.assertIn("必须且只能有1个参数", str(context.exception))


if __name__ == "__main__":
    unittest.main()
