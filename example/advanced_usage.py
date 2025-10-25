"""
高级使用示例

展示如何使用antchain库中的新操作符和高级功能。
"""

import random
from antchain import (
    Start,
    DATA,
    COUNT,
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

    # 数据过滤和处理
    print("\n2. 高价值销售分析:")
    start3 = Start()
    high_value_sales = (
        start3 | generate_sales_data | (DATA - filter_high_value_sales) | COUNT
    )
    print(f"   高价值销售记录数: {high_value_sales()}")

    print("\n=== 示例完成 ===")


if __name__ == "__main__":
    main()
