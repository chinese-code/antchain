"""
高级使用示例

展示如何使用antchain库中的新操作符和高级功能。
"""

import random
from antchain import (
    Start,
    DATA,
    COUNT,
    FIRST,
    LAST,
    MAX,
    MIN,
    SUM,
    AVG,
)


def generate_sales_data():
    """生成销售数据"""
    products = ["产品A", "产品B", "产品C", "产品D", "产品E"]
    return [
        {
            "id": i,
            "product": random.choice(products),
            "amount": random.randint(100, 1000),
            "quantity": random.randint(1, 20),
        }
        for i in range(1, 21)
    ]


def filter_high_value_sales(sale):
    """过滤高价值销售（金额大于500）"""
    return sale["amount"] > 500


def extract_amounts(sales):
    """提取金额列表"""
    return [sale["amount"] for sale in sales]


def main():
    print("=== AntChain高级使用示例 ===\n")

    # 基本数据处理
    print("1. 基本统计信息:")
    start = Start()
    sales_data = start | generate_sales_data
    total_sales = sales_data | COUNT
    print(f"   总销售记录数: {total_sales()}")

    # 提取金额列表
    start2 = Start()
    amount_list = sales_data | (DATA >> extract_amounts)
    max_amount = amount_list | MAX
    min_amount = amount_list | MIN
    avg_amount = amount_list | AVG
    sum_amount = amount_list | SUM

    print(f"   最大销售金额: {max_amount()}")
    print(f"   最小销售金额: {min_amount()}")
    print(f"   平均销售金额: {avg_amount():.2f}")
    print(f"   总销售金额: {sum_amount()}")

    # 数据过滤和处理
    print("\n2. 高价值销售分析:")
    start3 = Start()
    high_value_sales = sales_data | (DATA - filter_high_value_sales) | COUNT
    print(f"   高价值销售记录数: {high_value_sales()}")

    # 获取特定数据
    print("\n3. 特定数据获取:")
    start4 = Start()
    first_sale_amount = sales_data | (DATA >> extract_amounts) | FIRST
    last_sale_amount = sales_data | (DATA >> extract_amounts) | LAST
    print(f"   第一笔销售金额: {first_sale_amount()}")
    print(f"   最后一笔销售金额: {last_sale_amount()}")

    print("\n=== 示例完成 ===")


if __name__ == "__main__":
    main()
