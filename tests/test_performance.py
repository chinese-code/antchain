import time
import random
from antchain import Start, DATA, COUNT


def generate_large_dataset(size=10000):
    """生成大型数据集用于性能测试"""
    return [{"id": i, "value": random.randint(1, 1000)} for i in range(size)]


def process_batch(items, stream_size=100):
    """批处理函数"""
    # 模拟一些处理操作
    processed = []
    for item in items:
        processed.append({**item, "processed": True, "doubled": item["value"] * 2})
    return processed


def filter_items(item):
    """过滤函数"""
    return item["value"] > 500


def test_batch_processing_performance():
    """测试批处理性能"""
    print("=== 批处理性能测试 ===")
    # 生成测试数据
    data = generate_large_dataset(10000)

    # 测试批处理性能
    start_time = time.time()

    start = Start()
    chain = (
        start | (lambda: data) | (DATA >> process_batch) | (DATA - filter_items) | COUNT
    )

    result = chain()
    end_time = time.time()

    print(f"处理10000条数据耗时: {end_time - start_time:.4f}秒")
    print(f"结果数量: {result}")

    # 验证结果正确性
    assert isinstance(result, int)
    assert result >= 0
    print("✓ 批处理性能测试通过")


def test_join_performance():
    """测试连接性能"""
    print("\n=== 连接性能测试 ===")
    # 生成测试数据
    left_data = [{"id": i, "name": f"User{i}"} for i in range(1000)]
    right_data = [
        {"user_id": i, "score": random.randint(1, 100)} for i in range(0, 1000, 2)
    ]  # 只有一半的用户有分数

    def get_right_data(rows, stream_size=100):
        return right_data

    def join_func(
        left_key=lambda x: x["id"],
        right_key=lambda x: x["user_id"],
        left_property="score_info",
        one_to_many=False,
    ):
        pass

    start_time = time.time()

    start = Start()
    chain = start | (lambda: left_data) | ((DATA & get_right_data) * join_func) | COUNT

    result = chain()
    end_time = time.time()

    print(f"连接1000条数据耗时: {end_time - start_time:.4f}秒")
    print(f"结果数量: {result}")

    # 验证结果正确性
    assert isinstance(result, int)
    assert result == 1000  # 应该返回所有左边的数据
    print("✓ 连接性能测试通过")


def test_large_batch_processing():
    """测试大批次处理性能"""
    print("\n=== 大批次处理性能测试 ===")
    # 生成更大的测试数据
    data = generate_large_dataset(50000)

    def large_batch_process(items, stream_size=500):
        """大批次处理函数"""
        return [{"id": item["id"], "processed": True} for item in items]

    start_time = time.time()

    start = Start()
    chain = start | (lambda: data) | (DATA >> large_batch_process) | COUNT

    result = chain()
    end_time = time.time()

    print(f"处理50000条数据耗时: {end_time - start_time:.4f}秒")
    print(f"结果数量: {result}")

    assert isinstance(result, int)
    assert result == 50000
    print("✓ 大批次处理性能测试通过")


if __name__ == "__main__":
    print("开始性能测试...")
    test_batch_processing_performance()
    test_join_performance()
    test_large_batch_processing()
    print("\n所有性能测试完成!")
