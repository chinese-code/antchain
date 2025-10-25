import unittest
from antchain.utils import (
    get_function_args_count,
    get_stream_size,
    batch_process_data,
    get_function_return_type,
    mapping,
    group_by,
)


def sample_func_no_args():
    pass


def sample_func_one_arg(arg1):
    pass


def sample_func_two_args(arg1, arg2):
    pass


def sample_func_with_defaults(arg1, arg2=10):
    pass


def sample_func_with_stream_size(items, stream_size=5):
    return items


def sample_func_without_stream_size(items):
    return items


def sample_func_with_return_type(items) -> list:
    return items


def sample_func_without_return_type(items):
    return items


class TestUtils(unittest.TestCase):

    def test_get_function_args_count(self):
        """测试获取函数参数个数"""
        self.assertEqual(get_function_args_count(sample_func_no_args), 0)
        self.assertEqual(get_function_args_count(sample_func_one_arg), 1)
        self.assertEqual(get_function_args_count(sample_func_two_args), 2)
        self.assertEqual(
            get_function_args_count(sample_func_with_defaults), 1
        )  # 只计算无默认值的参数

    def test_get_stream_size(self):
        """测试获取stream_size"""
        self.assertEqual(get_stream_size(sample_func_with_stream_size), 5)
        self.assertEqual(get_stream_size(sample_func_without_stream_size), 0)

    def test_batch_process_data(self):
        """测试批处理数据"""
        data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

        # 测试无stream_size的情况
        result = batch_process_data(data, sample_func_without_stream_size)
        self.assertEqual(result, data)

        # 测试有stream_size的情况
        def process_with_size(items, stream_size=3):
            return [x * 2 for x in items]

        result = batch_process_data(data, process_with_size)
        expected = [2, 4, 6, 8, 10, 12, 14, 16, 18, 20]
        self.assertEqual(result, expected)

    def test_get_function_return_type(self):
        """测试获取函数返回类型"""
        self.assertEqual(get_function_return_type(sample_func_with_return_type), list)
        self.assertIsNone(get_function_return_type(sample_func_without_return_type))

    def test_mapping(self):
        """测试mapping函数"""
        data = [{"id": 1, "name": "A"}, {"id": 2, "name": "B"}]
        result = mapping(data, lambda x: x["id"])
        expected = {1: {"id": 1, "name": "A"}, 2: {"id": 2, "name": "B"}}
        self.assertEqual(result, expected)

    def test_group_by(self):
        """测试group_by函数"""
        data = [
            {"type": "A", "value": 1},
            {"type": "B", "value": 2},
            {"type": "A", "value": 3},
        ]
        result = group_by(data, lambda x: x["type"])
        expected = {
            "A": [{"type": "A", "value": 1}, {"type": "A", "value": 3}],
            "B": [{"type": "B", "value": 2}],
        }
        self.assertEqual(result, expected)


if __name__ == "__main__":
    unittest.main()
