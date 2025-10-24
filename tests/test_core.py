import pytest
from typing import List, Dict, Any
from datastream.core import OPMode, Stream, Start, DATA, PEEK, LIST, SET, COUNT, TUPLE, FIRST, LAST
from datastream.strategies import StrategyFactory
import inspect


def sample_init_data():
    """测试用的初始数据函数"""
    return [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]


def sample_process_item(item):
    """测试用的单条数据处理函数"""
    return {"id": item["id"], "processed": True}


def sample_process_batch(items):
    """测试用的批量数据处理函数"""
    return [item for item in items]


def sample_filter_func(item):
    """测试用的过滤函数"""
    return item["id"] % 2 == 0


def sample_merge_data():
    """测试用的合并数据函数"""
    return [{"id": 3, "name": "Charlie"}]


def sample_join_condition(left, right):
    """测试用的连接条件函数"""
    return left["id"] == right["id"]


def sample_join_data():
    """测试用的连接数据函数"""
    return [{"id": 1, "info": "data1"}, {"id": 2, "info": "data2"}]


def sample_join_data_func(stream_join=sample_join_condition):
    """测试用的连接数据函数（带stream_j_oin参数）"""
    return [{"id": 1, "info": "data1"}, {"id": 2, "info": "data2"}]


class TestOPMode:
    """测试OPMode类"""

    def test_opmode_init(self):
        """测试OPMode初始化"""
        op = OPMode("test")
        assert op.mode == "test"
        
        op = OPMode()
        assert op.mode is None

    def test_opmode_gt_operator(self):
        """测试>操作符"""
        result = DATA > sample_process_item
        assert isinstance(result, tuple)
        assert result[0] == "one"
        assert result[1] == sample_process_item

    def test_opmode_rshift_operator(self):
        """测试>>操作符"""
        result = DATA >> sample_process_batch
        assert isinstance(result, tuple)
        assert result[0] == "list"
        assert result[1] == sample_process_batch

    def test_opmode_sub_operator(self):
        """测试-操作符"""
        result = DATA - sample_filter_func
        assert isinstance(result, tuple)
        assert result[0] == "filter"
        assert result[1] == sample_filter_func

    def test_opmode_add_operator(self):
        """测试+操作符"""
        result = DATA + sample_merge_data
        assert isinstance(result, tuple)
        assert result[0] == "merge"
        assert result[1] == sample_merge_data

    def test_opmode_mul_operator_tuple(self):
        """测试*操作符（元组模式）"""
        result = DATA * (sample_join_condition, sample_join_data)
        assert isinstance(result, tuple)
        assert result[0] == "left_join"
        assert result[1] == sample_join_condition
        assert result[2] == sample_join_data

    def test_opmode_mul_operator_callable(self):
        """测试*操作符（可调用模式）"""
        def join_func_with_param(stream_join=sample_join_condition):
            return [{"id": 1, "info": "data1"}]
            
        result = DATA * join_func_with_param
        assert isinstance(result, tuple)
        assert result[0] == "left_join"
        assert hasattr(result[1], "_extract_func")
        assert hasattr(result[2], "_extract_func")

    def test_opmode_mul_operator_invalid(self):
        """测试*操作符（无效参数）"""
        # 创建一个不可调用的对象来触发TypeError
        class NotCallable:
            pass
            
        with pytest.raises(TypeError):
            # 传递一个不是元组也不是可调用的对象
            DATA * NotCallable()

    def test_opmode_pow_operator_tuple(self):
        """测试**操作符（元组模式）"""
        result = DATA ** (sample_join_condition, sample_join_data)
        assert isinstance(result, tuple)
        assert result[0] == "full_join"
        assert result[1] == sample_join_condition
        assert result[2] == sample_join_data

    def test_opmode_pow_operator_callable(self):
        """测试**操作符（可调用模式）"""
        def join_func_with_param(stream_join=sample_join_condition):
            return [{"id": 1, "info": "data1"}]
            
        result = DATA ** join_func_with_param
        assert isinstance(result, tuple)
        assert result[0] == "full_join"
        assert hasattr(result[1], "_extract_func")
        assert hasattr(result[2], "_extract_func")

    def test_opmode_pow_operator_invalid(self):
        """测试**操作符（无效参数）"""
        # 创建一个不可调用的对象来触发TypeError
        class NotCallable:
            pass
            
        with pytest.raises(TypeError):
            # 传递一个不是元组也不是可调用的对象
            DATA ** NotCallable()

    # 新增测试用例以提高覆盖率
    def test_opmode_mul_operator_callable_no_stream_join(self):
        """测试*操作符（可调用模式，无stream_join参数）"""
        def join_func_without_param():
            return [{"id": 1, "info": "data1"}]
            
        result = DATA * join_func_without_param
        assert isinstance(result, tuple)
        assert result[0] == "left_join"
        assert hasattr(result[1], "_extract_func")
        assert hasattr(result[2], "_extract_func")

    def test_opmode_pow_operator_callable_no_stream_join(self):
        """测试**操作符（可调用模式，无stream_join参数）"""
        def join_func_without_param():
            return [{"id": 1, "info": "data1"}]
            
        result = DATA ** join_func_without_param
        assert isinstance(result, tuple)
        assert result[0] == "full_join"
        assert hasattr(result[1], "_extract_func")
        assert hasattr(result[2], "_extract_func")

    def test_opmode_mul_operator_callable_stream_join_not_callable(self):
        """测试*操作符（可调用模式，stream_join参数不可调用）"""
        def join_func_with_non_callable_param(stream_join="not_callable"):
            return [{"id": 1, "info": "data1"}]
            
        result = DATA * join_func_with_non_callable_param
        assert isinstance(result, tuple)
        assert result[0] == "left_join"
        assert hasattr(result[1], "_extract_func")
        assert hasattr(result[2], "_extract_func")

    def test_opmode_pow_operator_callable_stream_join_not_callable(self):
        """测试**操作符（可调用模式，stream_join参数不可调用）"""
        def join_func_with_non_callable_param(stream_join="not_callable"):
            return [{"id": 1, "info": "data1"}]
            
        result = DATA ** join_func_with_non_callable_param
        assert isinstance(result, tuple)
        assert result[0] == "full_join"
        assert hasattr(result[1], "_extract_func")
        assert hasattr(result[2], "_extract_func")

    def test_opmode_mul_operator_callable_extract_condition_data_exception(self):
        """测试*操作符（可调用模式，提取条件数据时异常）"""
        def problematic_func():
            raise ValueError("Test exception")
            
        result = DATA * problematic_func
        assert isinstance(result, tuple)
        assert result[0] == "left_join"
        assert hasattr(result[1], "_extract_func")
        assert hasattr(result[2], "_extract_func")

    def test_opmode_pow_operator_callable_extract_condition_data_exception(self):
        """测试**操作符（可调用模式，提取条件数据时异常）"""
        def problematic_func():
            raise ValueError("Test exception")
            
        result = DATA ** problematic_func
        assert isinstance(result, tuple)
        assert result[0] == "full_join"
        assert hasattr(result[1], "_extract_func")
        assert hasattr(result[2], "_extract_func")

    def test_opmode_mul_operator_callable_with_callable_stream_join(self):
        """测试*操作符（可调用模式，stream_join参数可调用）"""
        def condition_func(left, right):
            return left["id"] == right["id"]
            
        def join_func_with_callable_param(stream_join=condition_func):
            return [{"id": 1, "info": "data1"}]
            
        result = DATA * join_func_with_callable_param
        assert isinstance(result, tuple)
        assert result[0] == "left_join"
        assert hasattr(result[1], "_extract_func")
        assert hasattr(result[2], "_extract_func")

    def test_opmode_pow_operator_callable_with_callable_stream_join(self):
        """测试**操作符（可调用模式，stream_join参数可调用）"""
        def condition_func(left, right):
            return left["id"] == right["id"]
            
        def join_func_with_callable_param(stream_join=condition_func):
            return [{"id": 1, "info": "data1"}]
            
        result = DATA ** join_func_with_callable_param
        assert isinstance(result, tuple)
        assert result[0] == "full_join"
        assert hasattr(result[1], "_extract_func")
        assert hasattr(result[2], "_extract_func")

    def test_opmode_mul_operator_callable_with_sample_join_condition(self):
        """测试*操作符（可调用模式，stream_join参数为sample_join_condition函数）"""
        def join_func_with_sample_condition(stream_join=sample_join_condition):
            return [{"id": 1, "info": "data1"}]
            
        result = DATA * join_func_with_sample_condition
        assert isinstance(result, tuple)
        assert result[0] == "left_join"
        assert hasattr(result[1], "_extract_func")
        assert hasattr(result[2], "_extract_func")

    def test_opmode_pow_operator_callable_with_sample_join_condition(self):
        """测试**操作符（可调用模式，stream_join参数为sample_join_condition函数）"""
        def join_func_with_sample_condition(stream_join=sample_join_condition):
            return [{"id": 1, "info": "data1"}]
            
        result = DATA ** join_func_with_sample_condition
        assert isinstance(result, tuple)
        assert result[0] == "full_join"
        assert hasattr(result[1], "_extract_func")
        assert hasattr(result[2], "_extract_func")


class TestStream:
    """测试Stream类"""

    def test_stream_init(self):
        """测试Stream初始化"""
        stream = Stream(sample_init_data, 50, "test_mode")
        assert stream.func == sample_init_data
        assert stream.batch_size == 50
        assert stream.process_mode == "test_mode"

    def test_stream_call(self):
        """测试Stream调用"""
        stream = Stream(sample_init_data)
        result = stream()
        assert result == [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]

    def test_stream_or_with_tuple_strategy(self):
        """测试Stream | 操作符（策略元组）"""
        stream = Stream(sample_init_data)
        new_stream = stream | (DATA > sample_process_item)
        assert isinstance(new_stream, Stream)
        result = new_stream()
        expected = [{"id": 1, "processed": True}, {"id": 2, "processed": True}]
        assert result == expected

    def test_stream_or_with_callable(self):
        """测试Stream | 操作符（可调用函数）"""
        stream = Stream(sample_init_data)
        new_stream = stream | sample_process_batch
        assert isinstance(new_stream, Stream)
        result = new_stream()
        expected = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
        assert result == expected

    def test_stream_or_invalid_type(self):
        """测试Stream | 操作符（无效类型）"""
        stream = Stream(sample_init_data)
        # 创建一个不可调用的对象来触发TypeError
        class NotCallable:
            pass
            
        with pytest.raises(TypeError):
            # 传递一个不可调用的对象
            stream | NotCallable()

    # 新增测试用例以提高覆盖率
    def test_stream_or_with_different_strategies(self):
        """测试Stream | 操作符（不同策略）"""
        stream = Stream(sample_init_data)
        
        # 测试 >> 操作符
        new_stream = stream | (DATA >> sample_process_batch)
        assert isinstance(new_stream, Stream)
        
        # 测试 - 操作符
        new_stream = stream | (DATA - sample_filter_func)
        assert isinstance(new_stream, Stream)
        
        # 测试 + 操作符
        new_stream = stream | (DATA + sample_merge_data)
        assert isinstance(new_stream, Stream)
        
        # 测试 * 操作符
        new_stream = stream | (DATA * (sample_join_condition, sample_join_data))
        assert isinstance(new_stream, Stream)
        
        # 测试 ** 操作符
        new_stream = stream | (DATA ** (sample_join_condition, sample_join_data))
        assert isinstance(new_stream, Stream)

    def test_stream_composed_with_strategy(self):
        """测试带策略的组合函数"""
        stream = Stream(sample_init_data)
        new_stream = stream | (DATA > sample_process_item)
        # 测试组合函数的执行
        result = new_stream()
        expected = [{"id": 1, "processed": True}, {"id": 2, "processed": True}]
        assert result == expected


class TestStart:
    """测试Start类"""

    def test_start_init(self):
        """测试Start初始化"""
        start = Start(50)
        assert start.size == 50
        
        start = Start()
        assert start.size == 100

    def test_start_or_with_callable(self):
        """测试Start | 操作符（可调用函数）"""
        start = Start()
        stream = start | sample_init_data
        assert isinstance(stream, Stream)
        result = stream()
        assert result == [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]

    def test_start_or_invalid_type(self):
        """测试Start | 操作符（无效类型）"""
        start = Start()
        # 创建一个不可调用的对象来触发TypeError
        class NotCallable:
            pass
            
        with pytest.raises(TypeError):
            # 传递一个不可调用的对象
            start | NotCallable()


class TestBuiltInCollectors:
    """测试内置收集器"""

    def test_peek(self):
        """测试PEEK收集器"""
        start = Start()
        # PEEK会打印数据，我们只验证它能正常工作
        stream = start | sample_init_data | PEEK
        result = stream()
        assert result == [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]

    def test_list(self):
        """测试LIST收集器"""
        start = Start()
        stream = start | sample_init_data | LIST
        result = stream()
        assert result == [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]

    def test_set(self):
        """测试SET收集器"""
        def get_duplicate_data():
            # 使用可哈希的元组而不是字典
            return [(1,), (1,), (2,)]
            
        start = Start()
        stream = start | get_duplicate_data | SET
        result = stream()
        assert isinstance(result, set)
        assert len(result) == 2

    def test_count(self):
        """测试COUNT收集器"""
        start = Start()
        stream = start | sample_init_data | COUNT
        result = stream()
        assert result == 2

    def test_tuple(self):
        """测试TUPLE收集器"""
        start = Start()
        stream = start | sample_init_data | TUPLE
        result = stream()
        assert isinstance(result, tuple)
        assert len(result) == 2

    def test_first(self):
        """测试FIRST收集器"""
        start = Start()
        stream = start | sample_init_data | FIRST
        result = stream()
        assert result == {"id": 1, "name": "Alice"}

    def test_first_empty(self):
        """测试FIRST收集器（空数据）"""
        def get_empty_data():
            return []
            
        start = Start()
        stream = start | get_empty_data | FIRST
        result = stream()
        assert result is None

    def test_last(self):
        """测试LAST收集器"""
        start = Start()
        stream = start | sample_init_data | LAST
        result = stream()
        assert result == {"id": 2, "name": "Bob"}

    def test_last_empty(self):
        """测试LAST收集器（空数据）"""
        def get_empty_data():
            return []
            
        start = Start()
        stream = start | get_empty_data | LAST
        result = stream()
        assert result is None


class TestIntegration:
    """集成测试类"""

    def test_full_join_integration_with_sample_join_condition(self):
        """测试全连接集成（使用sample_join_condition函数作为stream_join参数）"""
        start = Start()
        result = (
            start
            | sample_init_data
            | (DATA > sample_process_item)
            | (DATA ** (sample_join_condition, sample_join_data))
            | LIST
        )
        result_data = result()
        # 验证结果
        assert isinstance(result_data, list)
        assert len(result_data) == 2
        # 验证连接结果
        item1 = next((item for item in result_data if item["id"] == 1), None)
        item2 = next((item for item in result_data if item["id"] == 2), None)
        assert item1 is not None
        assert item2 is not None
        assert item1["info"] == "data1"
        assert item2["info"] == "data2"

    def test_left_join_integration_with_sample_join_condition(self):
        """测试左连接集成（使用sample_join_condition函数作为stream_join参数）"""
        start = Start()
        result = (
            start
            | sample_init_data
            | (DATA > sample_process_item)
            | (DATA * (sample_join_condition, sample_join_data))
            | LIST
        )
        result_data = result()
        # 验证结果
        assert isinstance(result_data, list)
        assert len(result_data) == 2
        # 验证连接结果
        item1 = next((item for item in result_data if item["id"] == 1), None)
        item2 = next((item for item in result_data if item["id"] == 2), None)
        assert item1 is not None
        assert item2 is not None
        assert item1["info"] == "data1"
        assert item2["info"] == "data2"

    def test_full_join_single_function_integration(self):
        """测试全连接单函数集成"""
        start = Start()
        result = (
            start
            | sample_init_data
            | (DATA > sample_process_item)
            | (DATA ** sample_join_data_func)
            | LIST
        )
        result_data = result()
        # 验证结果
        assert isinstance(result_data, list)
        assert len(result_data) == 2
        # 验证连接结果
        item1 = next((item for item in result_data if item["id"] == 1), None)
        item2 = next((item for item in result_data if item["id"] == 2), None)
        assert item1 is not None
        assert item2 is not None
        assert item1["info"] == "data1"
        assert item2["info"] == "data2"

    def test_left_join_single_function_integration(self):
        """测试左连接单函数集成"""
        start = Start()
        result = (
            start
            | sample_init_data
            | (DATA > sample_process_item)
            | (DATA * sample_join_data_func)
            | LIST
        )
        result_data = result()
        # 验证结果
        assert isinstance(result_data, list)
        assert len(result_data) == 2
        # 验证连接结果
        item1 = next((item for item in result_data if item["id"] == 1), None)
        item2 = next((item for item in result_data if item["id"] == 2), None)
        assert item1 is not None
        assert item2 is not None
        assert item1["info"] == "data1"
        assert item2["info"] == "data2"