import unittest
from antchain.validators import (
    validate_function_args_count,
    validate_filter_function,
    validate_join_conditions,
)
from antchain.exceptions import ValidationError, JoinError


def sample_func_no_args():
    pass


def sample_func_one_arg(arg1):
    pass


def sample_func_two_args(arg1, arg2):
    pass


def sample_filter_func_valid(item) -> bool:
    return True


def sample_filter_func_invalid_return(item) -> str:
    return "invalid"


def sample_filter_func_too_many_args(item1, item2) -> bool:
    return True


class TestValidators(unittest.TestCase):

    def test_validate_function_args_count_valid(self):
        """测试验证函数参数个数 - 有效情况"""
        # 应该不抛出异常
        validate_function_args_count(sample_func_no_args, 0, "测试")
        validate_function_args_count(sample_func_one_arg, 1, "测试")
        validate_function_args_count(sample_func_two_args, 2, "测试")

    def test_validate_function_args_count_invalid(self):
        """测试验证函数参数个数 - 无效情况"""
        with self.assertRaises(ValidationError) as context:
            validate_function_args_count(sample_func_one_arg, 2, "测试操作")

        self.assertIn("测试操作", str(context.exception))
        self.assertIn("必须且只能有2个参数", str(context.exception))

    def test_validate_filter_function_valid(self):
        """测试验证过滤函数 - 有效情况"""
        # 应该不抛出异常
        validate_filter_function(sample_filter_func_valid)

    def test_validate_filter_function_invalid_return_type(self):
        """测试验证过滤函数 - 无效返回类型"""
        with self.assertRaises(ValidationError) as context:
            validate_filter_function(sample_filter_func_invalid_return)

        self.assertIn("过滤函数", str(context.exception))
        self.assertIn("返回值类型必须是bool", str(context.exception))

    def test_validate_filter_function_too_many_args(self):
        """测试验证过滤函数 - 参数过多"""
        with self.assertRaises(ValidationError) as context:
            validate_filter_function(sample_filter_func_too_many_args)

        self.assertIn("过滤函数", str(context.exception))
        self.assertIn("最多只能有一个参数", str(context.exception))

    def test_validate_join_conditions_valid(self):
        """测试验证连接条件 - 有效情况"""

        # 创建一个模拟的element对象
        class MockElement:
            def __init__(self):
                self.right_func = lambda x: x

        element = MockElement()
        left_key = lambda x: x
        right_key = lambda x: x
        one_to_many = True

        # 应该不抛出异常
        validate_join_conditions(element, left_key, right_key, one_to_many)

    def test_validate_join_conditions_invalid_right_func(self):
        """测试验证连接条件 - 无效右函数"""

        class MockElement:
            def __init__(self):
                self.right_func = None

        element = MockElement()
        left_key = lambda x: x
        right_key = lambda x: x
        one_to_many = True

        with self.assertRaises(JoinError) as context:
            validate_join_conditions(element, left_key, right_key, one_to_many)

        self.assertIn("& 运算符的右边不能为空", str(context.exception))

    def test_validate_join_conditions_invalid_left_key(self):
        """测试验证连接条件 - 无效左键"""

        class MockElement:
            def __init__(self):
                self.right_func = lambda x: x

        element = MockElement()
        left_key = None
        right_key = lambda x: x
        one_to_many = True

        with self.assertRaises(JoinError) as context:
            validate_join_conditions(element, left_key, right_key, one_to_many)

        self.assertIn("left_key 不能为空", str(context.exception))

    def test_validate_join_conditions_invalid_right_key(self):
        """测试验证连接条件 - 无效右键"""

        class MockElement:
            def __init__(self):
                self.right_func = lambda x: x

        element = MockElement()
        left_key = lambda x: x
        right_key = None
        one_to_many = True

        with self.assertRaises(JoinError) as context:
            validate_join_conditions(element, left_key, right_key, one_to_many)

        self.assertIn("right_key 不能为空", str(context.exception))

    def test_validate_join_conditions_invalid_one_to_many(self):
        """测试验证连接条件 - 无效一对多标志"""

        class MockElement:
            def __init__(self):
                self.right_func = lambda x: x

        element = MockElement()
        left_key = lambda x: x
        right_key = lambda x: x
        one_to_many = None

        with self.assertRaises(JoinError) as context:
            validate_join_conditions(element, left_key, right_key, one_to_many)

        self.assertIn("one_to_many 不能为空", str(context.exception))


if __name__ == "__main__":
    unittest.main()
