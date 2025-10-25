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
    filter_none,
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

        self.assertEqual(len(stream1.child_nodes), 1)
        self.assertEqual(stream1.child_nodes[0].element, element2)

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


if __name__ == "__main__":
    unittest.main()
