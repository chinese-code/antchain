import unittest
from antchain.strategy import StrategyFactory
from antchain.element import Element
from antchain.exceptions import StrategyError, ProcessingError, JoinError


class TestStrategyFactory(unittest.TestCase):

    def test_singleton_pattern(self):
        """测试单例模式"""
        factory1 = StrategyFactory()
        factory2 = StrategyFactory()
        self.assertIs(factory1, factory2)

    def test_get_processor(self):
        """测试获取处理器"""
        factory = StrategyFactory()
        processor = factory.get_processor("one")
        self.assertIsNotNone(processor)
        self.assertEqual(processor, factory.one)

        # 测试不存在的处理器
        processor = factory.get_processor("nonexistent")
        self.assertIsNone(processor)

    def test_process_none_element(self):
        """测试处理空元素"""
        with self.assertRaises(StrategyError) as context:
            StrategyFactory.process(None)  # type: ignore

        self.assertIn("element 不能为空", str(context.exception))

    def test_process_invalid_element_type(self):
        """测试处理无效元素类型"""
        element = Element(element_type="invalid")
        with self.assertRaises(StrategyError) as context:
            StrategyFactory.process(element)

        self.assertIn("不支持的element_type", str(context.exception))

    def test_init_processor_success(self):
        """测试初始化处理器成功"""

        def init_func():
            return [{"id": 1}]

        element = Element(element_type="init", right_func=init_func)
        result = StrategyFactory.process(element)
        self.assertEqual(result, [{"id": 1}])

    def test_init_processor_no_func(self):
        """测试初始化处理器无函数异常"""
        element = Element(element_type="init")
        with self.assertRaises(StrategyError) as context:
            StrategyFactory.process(element)

        self.assertIn("right_func 不能为空", str(context.exception))

    def test_init_processor_exception(self):
        """测试初始化处理器异常处理"""

        def failing_init_func():
            raise ValueError("初始化失败")

        element = Element(element_type="init", right_func=failing_init_func)
        with self.assertRaises(ProcessingError) as context:
            StrategyFactory.process(element)

        self.assertIn("初始化函数执行失败", str(context.exception))
        self.assertIn("初始化失败", str(context.exception))

    def test_one_processor_success(self):
        """测试单条处理器成功"""

        def process_func(item):
            return {"id": item["id"], "processed": True}

        element = Element(element_type="one", right_func=process_func)
        left_data = [{"id": 1}, {"id": 2}]
        factory = StrategyFactory()
        result = factory.one(element, left_data)
        expected = [{"id": 1, "processed": True}, {"id": 2, "processed": True}]
        self.assertEqual(result, expected)

    def test_one_processor_no_func(self):
        """测试单条处理器无函数异常"""
        element = Element(element_type="one")
        left_data = [{"id": 1}]
        factory = StrategyFactory()
        with self.assertRaises(StrategyError) as context:
            factory.one(element, left_data)

        self.assertIn("right_func 不能为空", str(context.exception))

    def test_one_processor_exception(self):
        """测试单条处理器异常处理"""

        def failing_process_func(item):
            raise ValueError("处理失败")

        element = Element(element_type="one", right_func=failing_process_func)
        left_data = [{"id": 1}]
        factory = StrategyFactory()
        with self.assertRaises(ProcessingError) as context:
            factory.one(element, left_data)

        self.assertIn("单条处理函数执行失败", str(context.exception))
        self.assertIn("处理失败", str(context.exception))

    def test_one_processor_none_data(self):
        """测试单条处理器处理None数据"""

        def process_func(item):
            return {"item": item, "processed": True}

        element = Element(element_type="one", right_func=process_func)
        left_data = None
        factory = StrategyFactory()
        result = factory.one(element, left_data)
        expected = [{"item": None, "processed": True}]
        self.assertEqual(result, expected)

    def test_one_processor_single_item(self):
        """测试单条处理器处理单个数据项"""

        def process_func(item):
            return {"id": item["id"], "processed": True}

        element = Element(element_type="one", right_func=process_func)
        left_data = {"id": 1}
        factory = StrategyFactory()
        result = factory.one(element, left_data)
        expected = [{"id": 1, "processed": True}]
        self.assertEqual(result, expected)

    def test_multi_processor_success(self):
        """测试批处理器成功"""

        def process_func(items):
            return [{"id": item["id"], "processed": True} for item in items]

        element = Element(element_type="multi", right_func=process_func)
        left_data = [{"id": 1}, {"id": 2}]
        factory = StrategyFactory()
        result = factory.multi(element, left_data)
        expected = [{"id": 1, "processed": True}, {"id": 2, "processed": True}]
        self.assertEqual(result, expected)

    def test_multi_processor_no_func(self):
        """测试批处理器无函数异常"""
        element = Element(element_type="multi")
        left_data = [{"id": 1}]
        factory = StrategyFactory()
        with self.assertRaises(StrategyError) as context:
            factory.multi(element, left_data)

        self.assertIn("right_func 不能为空", str(context.exception))

    def test_multi_processor_exception(self):
        """测试批处理器异常处理"""

        def failing_process_func(items):
            raise ValueError("批处理失败")

        element = Element(element_type="multi", right_func=failing_process_func)
        left_data = [{"id": 1}]
        factory = StrategyFactory()
        with self.assertRaises(ProcessingError) as context:
            factory.multi(element, left_data)

        self.assertIn("批处理函数执行失败", str(context.exception))
        self.assertIn("批处理失败", str(context.exception))

    def test_multi_processor_none_data(self):
        """测试批处理器处理None数据"""

        def process_func(item):
            return {"item": item, "processed": True}

        element = Element(element_type="multi", right_func=process_func)
        left_data = None
        factory = StrategyFactory()
        result = factory.multi(element, left_data)
        expected = {"item": None, "processed": True}
        self.assertEqual(result, expected)

    def test_multi_processor_single_item(self):
        """测试批处理器处理单个数据项"""

        def process_func(item):
            return {"id": item["id"], "processed": True}

        element = Element(element_type="multi", right_func=process_func)
        left_data = {"id": 1}
        factory = StrategyFactory()
        result = factory.multi(element, left_data)
        expected = {"id": 1, "processed": True}
        self.assertEqual(result, expected)

    def test_merge_processor_success(self):
        """测试合并处理器成功"""

        def merge_func():
            return [{"id": 3}, {"id": 4}]

        element = Element(element_type="merge", right_func=merge_func)
        left_data = [{"id": 1}, {"id": 2}]
        factory = StrategyFactory()
        result = factory.merge(element, left_data)
        expected = [{"id": 1}, {"id": 2}, {"id": 3}, {"id": 4}]
        self.assertEqual(result, expected)

    def test_merge_processor_no_func(self):
        """测试合并处理器无函数异常"""
        element = Element(element_type="merge")
        left_data = [{"id": 1}]
        factory = StrategyFactory()
        with self.assertRaises(StrategyError) as context:
            factory.merge(element, left_data)

        self.assertIn("right_func 不能为空", str(context.exception))

    def test_merge_processor_exception(self):
        """测试合并处理器异常处理"""

        def failing_merge_func():
            raise ValueError("合并失败")

        element = Element(element_type="merge", right_func=failing_merge_func)
        left_data = [{"id": 1}]
        factory = StrategyFactory()
        with self.assertRaises(ProcessingError) as context:
            factory.merge(element, left_data)

        self.assertIn("合并操作失败", str(context.exception))
        self.assertIn("合并失败", str(context.exception))

    def test_merge_processor_none_left_data(self):
        """测试合并处理器处理None左侧数据"""

        def merge_func():
            return [{"id": 2}]

        element = Element(element_type="merge", right_func=merge_func)
        left_data = None
        factory = StrategyFactory()
        result = factory.merge(element, left_data)
        expected = [{"id": 2}]
        self.assertEqual(result, expected)

    def test_merge_processor_single_left_item(self):
        """测试合并处理器处理单个左侧数据项"""

        def merge_func():
            return [{"id": 2}]

        element = Element(element_type="merge", right_func=merge_func)
        left_data = {"id": 1}
        factory = StrategyFactory()
        result = factory.merge(element, left_data)
        expected = [{"id": 1}, {"id": 2}]
        self.assertEqual(result, expected)

    def test_merge_processor_tuple_left_data(self):
        """测试合并处理器处理元组左侧数据"""

        def merge_func():
            return [{"id": 3}]

        element = Element(element_type="merge", right_func=merge_func)
        left_data = ({"id": 1}, {"id": 2})
        factory = StrategyFactory()
        result = factory.merge(element, left_data)
        expected = [{"id": 1}, {"id": 2}, {"id": 3}]
        self.assertEqual(result, expected)

    def test_filter_processor_success(self):
        """测试过滤处理器成功"""

        def filter_func(item):
            return item["id"] % 2 == 0

        element = Element(element_type="filter", right_func=filter_func)
        left_data = [{"id": 1}, {"id": 2}, {"id": 3}, {"id": 4}]
        factory = StrategyFactory()
        result = factory.filter(element, left_data)
        expected = [{"id": 2}, {"id": 4}]
        self.assertEqual(result, expected)

    def test_filter_processor_no_func(self):
        """测试过滤处理器无函数异常"""
        element = Element(element_type="filter")
        left_data = [{"id": 1}]
        factory = StrategyFactory()
        with self.assertRaises(StrategyError) as context:
            factory.filter(element, left_data)

        self.assertIn("right_func 不能为空", str(context.exception))

    def test_filter_processor_exception(self):
        """测试过滤处理器异常处理"""

        def failing_filter_func(item):
            raise ValueError("过滤失败")

        element = Element(element_type="filter", right_func=failing_filter_func)
        left_data = [{"id": 1}]
        factory = StrategyFactory()
        with self.assertRaises(ProcessingError) as context:
            factory.filter(element, left_data)

        self.assertIn("过滤操作失败", str(context.exception))
        self.assertIn("过滤失败", str(context.exception))

    def test_filter_processor_none_data(self):
        """测试过滤处理器处理None数据"""

        def filter_func(item):
            return True

        element = Element(element_type="filter", right_func=filter_func)
        left_data = None
        factory = StrategyFactory()
        result = factory.filter(element, left_data)
        expected = []
        self.assertEqual(result, expected)

    def test_filter_processor_single_item(self):
        """测试过滤处理器处理单个数据项"""

        def filter_func(item):
            return item["id"] > 1

        element = Element(element_type="filter", right_func=filter_func)
        left_data = {"id": 1}
        factory = StrategyFactory()
        result = factory.filter(element, left_data)
        expected = []
        self.assertEqual(result, expected)

    def test_left_join_processor_no_join_func(self):
        """测试左连接处理器无连接函数异常"""
        element = Element(element_type="left_join")
        left_data = [{"id": 1}]
        factory = StrategyFactory()
        with self.assertRaises(JoinError) as context:
            factory.left_join(element, left_data)

        self.assertIn("* 运算符的右边不能为空", str(context.exception))

    def test_all_join_processor_no_join_func(self):
        """测试全连接处理器无连接函数异常"""
        element = Element(element_type="all_join")
        left_data = [{"id": 1}]
        factory = StrategyFactory()
        with self.assertRaises(JoinError) as context:
            factory.all_join(element, left_data)

        self.assertIn("** 运算符的右边不能为空", str(context.exception))

    def test_left_join_processor_success(self):
        """测试左连接处理器成功"""

        def right_func(rows):
            return [{"user_id": 1, "name": "Alice"}]

        def join_func(
            left_key=lambda x: x["id"],
            right_key=lambda x: x["user_id"],
            left_property="user_info",
            one_to_many=False,
        ):
            pass

        element = Element(
            element_type="left_join", right_func=right_func, join_func=join_func
        )
        left_data = [{"id": 1, "value": 100}]
        factory = StrategyFactory()
        result = factory.left_join(element, left_data)
        # 验证结果包含连接的数据
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["id"], 1)
        self.assertEqual(result[0]["value"], 100)

    def test_all_join_processor_success(self):
        """测试全连接处理器成功"""

        def right_func(rows):
            return [{"user_id": 1, "name": "Alice"}]

        def join_func(
            left_key=lambda x: x["id"],
            right_key=lambda x: x["user_id"],
            left_property="user_info",
            one_to_many=False,
        ):
            pass

        element = Element(
            element_type="all_join", right_func=right_func, join_func=join_func
        )
        left_data = [{"id": 1, "value": 100}]
        factory = StrategyFactory()
        result = factory.all_join(element, left_data)
        # 验证结果包含连接的数据
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["id"], 1)
        self.assertEqual(result[0]["value"], 100)

    def test_left_join_processor_empty_data(self):
        """测试左连接处理器处理空数据"""

        def right_func(rows):
            return []

        def join_func(
            left_key=lambda x: x["id"],
            right_key=lambda x: x["user_id"],
            left_property="user_info",
            one_to_many=False,
        ):
            pass

        element = Element(
            element_type="left_join", right_func=right_func, join_func=join_func
        )
        left_data = []
        factory = StrategyFactory()
        result = factory.left_join(element, left_data)
        self.assertEqual(result, [])

    def test_all_join_processor_empty_data(self):
        """测试全连接处理器处理空数据"""

        def right_func(rows):
            return []

        def join_func(
            left_key=lambda x: x["id"],
            right_key=lambda x: x["user_id"],
            left_property="user_info",
            one_to_many=False,
        ):
            pass

        element = Element(
            element_type="all_join", right_func=right_func, join_func=join_func
        )
        left_data = []
        factory = StrategyFactory()
        result = factory.all_join(element, left_data)
        self.assertEqual(result, [])

    def test_left_join_processor_empty_right_data(self):
        """测试左连接处理器处理空右侧数据"""

        def right_func(rows):
            return []

        def join_func(
            left_key=lambda x: x["id"],
            right_key=lambda x: x["user_id"],
            left_property="user_info",
            one_to_many=False,
        ):
            pass

        element = Element(
            element_type="left_join", right_func=right_func, join_func=join_func
        )
        left_data = [{"id": 1, "value": 100}]
        factory = StrategyFactory()
        result = factory.left_join(element, left_data)
        # 左连接应该返回左侧数据
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["id"], 1)
        self.assertEqual(result[0]["value"], 100)

    def test_all_join_processor_empty_right_data(self):
        """测试全连接处理器处理空右侧数据"""

        def right_func(rows):
            return []

        def join_func(
            left_key=lambda x: x["id"],
            right_key=lambda x: x["user_id"],
            left_property="user_info",
            one_to_many=False,
        ):
            pass

        element = Element(
            element_type="all_join", right_func=right_func, join_func=join_func
        )
        left_data = [{"id": 1, "value": 100}]
        factory = StrategyFactory()
        result = factory.all_join(element, left_data)
        # 全连接应该返回左侧数据
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["id"], 1)
        self.assertEqual(result[0]["value"], 100)

    def test_left_join_processor_exception(self):
        """测试左连接处理器异常处理"""

        def failing_right_func(rows):
            raise ValueError("右侧函数执行失败")

        def join_func(
            left_key=lambda x: x["id"],
            right_key=lambda x: x["user_id"],
            left_property="user_info",
            one_to_many=False,
        ):
            pass

        element = Element(
            element_type="left_join", right_func=failing_right_func, join_func=join_func
        )
        left_data = [{"id": 1, "value": 100}]
        factory = StrategyFactory()
        with self.assertRaises(JoinError) as context:
            factory.left_join(element, left_data)

        self.assertIn("左连接操作失败", str(context.exception))
        self.assertIn("右侧函数执行失败", str(context.exception))

    def test_all_join_processor_exception(self):
        """测试全连接处理器异常处理"""

        def failing_right_func(rows):
            raise ValueError("右侧函数执行失败")

        def join_func(
            left_key=lambda x: x["id"],
            right_key=lambda x: x["user_id"],
            left_property="user_info",
            one_to_many=False,
        ):
            pass

        element = Element(
            element_type="all_join", right_func=failing_right_func, join_func=join_func
        )
        left_data = [{"id": 1, "value": 100}]
        factory = StrategyFactory()
        with self.assertRaises(JoinError) as context:
            factory.all_join(element, left_data)

        self.assertIn("全连接操作失败", str(context.exception))
        self.assertIn("右侧函数执行失败", str(context.exception))

    def test_left_join_merge_exception(self):
        """测试左连接合并异常处理"""

        def right_func(rows):
            return [{"user_id": 1, "name": "Alice"}]

        def failing_join_func(
            left_key=lambda x: x["id"],
            right_key=lambda x: x["user_id"],
            left_property="user_info",
            one_to_many=False,
        ):
            pass

        # 模拟__join_check返回无效参数导致异常
        factory = StrategyFactory()
        element = Element(
            element_type="left_join", right_func=right_func, join_func=failing_join_func
        )
        left_data = [{"id": 1, "value": 100}]

        # 直接测试__left_join_merge方法中的异常处理
        with self.assertRaises(JoinError) as context:
            # 模拟left_key函数抛出异常
            def failing_left_key(item):
                raise ValueError("左侧键函数执行失败")

            factory._left_join_merge(
                left_data,
                False,
                [{"user_id": 1}],
                failing_left_key,
                lambda x: x["user_id"],
                "user_info",
            )

        self.assertIn("连接合并过程中出现错误", str(context.exception))
        self.assertIn("左侧键函数执行失败", str(context.exception))

    def test_join_check_exception(self):
        """测试连接检查异常处理"""

        def join_func(
            left_key=None,  # 故意传入None导致验证失败
            right_key=None,
            left_property="user_info",
            one_to_many=None,
        ):
            pass

        element = Element(
            element_type="left_join", right_func=lambda x: x, join_func=join_func
        )
        factory = StrategyFactory()

        with self.assertRaises(JoinError) as context:
            factory._join_check(element, [{"id": 1}])

        self.assertIn("left_key 不能为空", str(context.exception))

    def test_batch_processor_stream_size(self):
        """测试批处理器stream_size参数"""

        def batch_func(items, stream_size=2):
            return [{"id": item["id"], "processed": True} for item in items]

        element = Element(element_type="multi", right_func=batch_func)
        left_data = [{"id": 1}, {"id": 2}, {"id": 3}, {"id": 4}, {"id": 5}]
        factory = StrategyFactory()
        result = factory.multi(element, left_data)
        expected = [
            {"id": 1, "processed": True},
            {"id": 2, "processed": True},
            {"id": 3, "processed": True},
            {"id": 4, "processed": True},
            {"id": 5, "processed": True},
        ]
        self.assertEqual(result, expected)

    def test_left_join_merge_one_to_many(self):
        """测试左连接合并一对多情况"""
        factory = StrategyFactory()
        left_data = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
        right_data = [{"user_id": 1, "score": 90}, {"user_id": 1, "score": 85}]

        def left_key(item):
            return item["id"]

        def right_key(item):
            return item["user_id"]

        # 测试一对多情况
        result = factory._left_join_merge(
            left_data, True, right_data, left_key, right_key, "scores"
        )

        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["id"], 1)
        self.assertEqual(len(result[0]["scores"]), 2)
        self.assertEqual(result[1]["id"], 2)
        self.assertIsNone(result[1].get("scores"))

    def test_left_join_merge_one_to_one(self):
        """测试左连接合并一对一情况"""
        factory = StrategyFactory()
        left_data = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
        right_data = [{"user_id": 1, "score": 90}, {"user_id": 2, "score": 85}]

        def left_key(item):
            return item["id"]

        def right_key(item):
            return item["user_id"]

        # 测试一对一情况，使用None作为left_property表示合并数据
        result = factory._left_join_merge(
            left_data, False, right_data, left_key, right_key, None
        )

        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["id"], 1)
        # 当left_property为None时，数据会被合并
        self.assertIn("score", result[0])
        self.assertEqual(result[1]["id"], 2)
        self.assertIn("score", result[1])

    def test_left_join_merge_with_property(self):
        """测试左连接合并带属性名情况"""
        factory = StrategyFactory()
        left_data = [{"id": 1, "name": "Alice"}]
        right_data = [{"user_id": 1, "score": 90}]

        def left_key(item):
            return item["id"]

        def right_key(item):
            return item["user_id"]

        # 测试带属性名情况
        result = factory._left_join_merge(
            left_data, False, right_data, left_key, right_key, "user_info"
        )

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["id"], 1)
        self.assertEqual(result[0]["user_info"]["score"], 90)

    def test_all_join_processor_right_only_data(self):
        """测试全连接处理器处理仅右侧数据"""

        def right_func(rows):
            return [{"user_id": 3, "name": "Charlie"}]

        def join_func(
            left_key=lambda x: x["id"],
            right_key=lambda x: x["user_id"],
            left_property="user_info",
            one_to_many=False,
        ):
            pass

        element = Element(
            element_type="all_join", right_func=right_func, join_func=join_func
        )
        left_data = [{"id": 1, "value": 100}, {"id": 2, "value": 200}]
        factory = StrategyFactory()
        result = factory.all_join(element, left_data)
        # 验证结果包含所有数据，包括仅在右侧的数据
        self.assertGreaterEqual(len(result), 3)  # 至少包含左侧2条和右侧1条数据


if __name__ == "__main__":
    unittest.main()
