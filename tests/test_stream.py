import unittest
from antchain.stream import (
    Stream,
    Start,
    Element,
    DATA,
    collect_list,
    collect_set,
    collect_count,
    collect_tuple,
    collect_first,
    collect_last,
    collect_max,
    collect_min,
    collect_sum,
    collect_avg,
    filter_none,
    PEEK,
    LIST,
    SET,
    COUNT,
    TUPLE,
    FIRST,
    LAST,
    NON,
    MAX,
    MIN,
    SUM,
    AVG,
)


class TestStream(unittest.TestCase):

    def test_stream_initialization(self):
        """测试Stream初始化"""
        element = Element()
        stream = Stream("init", element)
        self.assertEqual(stream.mode, "init")
        self.assertEqual(stream.element, element)
        self.assertEqual(stream.child_nodes, [])

    def test_start_initialization(self):
        """测试Start初始化"""
        start = Start()
        self.assertIsInstance(start, Start)

    def test_stream_or_operator(self):
        """测试Stream的|操作符"""
        element1 = Element()
        element2 = Element()
        stream1 = Stream("init", element1)
        stream2 = stream1 | element2

        # 现在stream1应该保持不变，而stream2应该是新的对象
        self.assertEqual(len(stream1.child_nodes), 0)
        self.assertEqual(len(stream2.child_nodes), 1)
        self.assertEqual(stream2.child_nodes[0].element, element2)
        # 验证返回的是不同的对象
        self.assertNotEqual(id(stream1), id(stream2))

    def test_collect_functions(self):
        """测试收集函数"""
        # 测试collect_list
        data = [1, 2, 3]
        result = collect_list(data)
        self.assertEqual(result, data)

        # 测试collect_set
        data = [1, 2, 2, 3]
        result = collect_set(data)
        self.assertEqual(len(result), 3)

        # 测试collect_count
        data = [1, 2, 3]
        result = collect_count(data)
        self.assertEqual(result, 3)

        # 测试collect_tuple
        data = [1, 2, 3]
        result = collect_tuple(data)
        self.assertEqual(result, (1, 2, 3))

        # 测试filter_none
        data = [1, None, 2, None, 3]
        result = [x for x in data if filter_none(x)]
        self.assertEqual(result, [1, 2, 3])

    def test_collect_first_function(self):
        """测试collect_first函数"""
        # 测试正常列表
        data = [1, 2, 3]
        result = collect_first(data)
        self.assertEqual(result, 1)

        # 测试空列表
        data = []
        result = collect_first(data)
        self.assertIsNone(result)

        # 测试单个元素
        data = [42]
        result = collect_first(data)
        self.assertEqual(result, 42)

        # 测试非列表数据
        data = "hello"
        result = collect_first(data)
        self.assertEqual(result, "hello")

        # 测试None数据
        data = None
        result = collect_first(data)
        self.assertIsNone(result)

    def test_collect_last_function(self):
        """测试collect_last函数"""
        # 测试正常列表
        data = [1, 2, 3]
        result = collect_last(data)
        self.assertEqual(result, 3)

        # 测试空列表
        data = []
        result = collect_last(data)
        self.assertIsNone(result)

        # 测试单个元素
        data = [42]
        result = collect_last(data)
        self.assertEqual(result, 42)

        # 测试非列表数据
        data = "hello"
        result = collect_last(data)
        self.assertEqual(result, "hello")

        # 测试None数据
        data = None
        result = collect_last(data)
        self.assertIsNone(result)

    def test_collect_max_function(self):
        """测试collect_max函数"""
        # 测试正常列表
        data = [1, 5, 3, 2]
        result = collect_max(data)
        self.assertEqual(result, 5)

        # 测试空列表
        data = []
        result = collect_max(data)
        self.assertIsNone(result)

        # 测试单个元素
        data = [42]
        result = collect_max(data)
        self.assertEqual(result, 42)

        # 测试非列表数据
        data = "hello"
        result = collect_max(data)
        self.assertEqual(result, "hello")

        # 测试None数据
        data = None
        result = collect_max(data)
        self.assertIsNone(result)

    def test_collect_min_function(self):
        """测试collect_min函数"""
        # 测试正常列表
        data = [5, 1, 3, 2]
        result = collect_min(data)
        self.assertEqual(result, 1)

        # 测试空列表
        data = []
        result = collect_min(data)
        self.assertIsNone(result)

        # 测试单个元素
        data = [42]
        result = collect_min(data)
        self.assertEqual(result, 42)

        # 测试非列表数据
        data = "hello"
        result = collect_min(data)
        self.assertEqual(result, "hello")

        # 测试None数据
        data = None
        result = collect_min(data)
        self.assertIsNone(result)

    def test_collect_sum_function(self):
        """测试collect_sum函数"""
        # 测试正常列表
        data = [1, 2, 3, 4]
        result = collect_sum(data)
        self.assertEqual(result, 10)

        # 测试空列表
        data = []
        result = collect_sum(data)
        self.assertEqual(result, 0)

        # 测试单个数字
        data = 42
        result = collect_sum(data)
        self.assertEqual(result, 42)

        # 测试None数据
        data = None
        result = collect_sum(data)
        self.assertEqual(result, 0)

    def test_collect_avg_function(self):
        """测试collect_avg函数"""
        # 测试正常列表
        data = [1, 2, 3, 4]
        result = collect_avg(data)
        self.assertEqual(result, 2.5)

        # 测试空列表
        data = []
        result = collect_avg(data)
        self.assertEqual(result, 0.0)

        # 测试单个数字
        data = 42
        result = collect_avg(data)
        self.assertEqual(result, 42.0)

        # 测试None数据
        data = None
        result = collect_avg(data)
        self.assertEqual(result, 0.0)

    def test_builtin_operators(self):
        """测试内置操作符"""
        # 测试DATA元素类型
        self.assertEqual(DATA.element_type, "data")

        # 测试PEEK操作符
        self.assertEqual(PEEK.element_type, "multi")
        self.assertIsNotNone(PEEK.right_func)

        # 测试LIST操作符
        self.assertEqual(LIST.element_type, "multi")
        self.assertIsNotNone(LIST.right_func)

        # 测试SET操作符
        self.assertEqual(SET.element_type, "multi")
        self.assertIsNotNone(SET.right_func)

        # 测试COUNT操作符
        self.assertEqual(COUNT.element_type, "multi")
        self.assertIsNotNone(COUNT.right_func)

        # 测试TUPLE操作符
        self.assertEqual(TUPLE.element_type, "multi")
        self.assertIsNotNone(TUPLE.right_func)

        # 测试FIRST操作符
        self.assertEqual(FIRST.element_type, "multi")
        self.assertIsNotNone(FIRST.right_func)

        # 测试LAST操作符
        self.assertEqual(LAST.element_type, "multi")
        self.assertIsNotNone(LAST.right_func)

        # 测试NON操作符
        self.assertEqual(NON.element_type, "filter")
        self.assertIsNotNone(NON.right_func)

        # 测试MAX操作符
        self.assertEqual(MAX.element_type, "multi")
        self.assertIsNotNone(MAX.right_func)

        # 测试MIN操作符
        self.assertEqual(MIN.element_type, "multi")
        self.assertIsNotNone(MIN.right_func)

        # 测试SUM操作符
        self.assertEqual(SUM.element_type, "multi")
        self.assertIsNotNone(SUM.right_func)

        # 测试AVG操作符
        self.assertEqual(AVG.element_type, "multi")
        self.assertIsNotNone(AVG.right_func)

    def test_start_or_operator(self):
        """测试Start的|操作符"""

        def test_func():
            return [1, 2, 3]

        start = Start()
        stream = start | test_func

        self.assertIsInstance(stream, Stream)
        self.assertEqual(stream.mode, "init")
        self.assertIsNotNone(stream.element)
        self.assertEqual(stream.element.element_type, "init")
        self.assertEqual(stream.element.right_func, test_func)


if __name__ == "__main__":
    unittest.main()
