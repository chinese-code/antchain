"""
数据流处理管道使用示例

展示如何使用stream包中的各种操作符和功能。
"""

from datastream import DATA, Start


def init_data():
    """初始化数据"""
    print("执行 init_data（初始函数）")
    return [{"name": "one", "id": 1}, {"name": "one", "id": 2}]


def process_single_item(row):
    """单条数据处理函数"""
    print(f"执行 process_single_item（单条处理），处理: {row}")
    return {"class_id": row["id"], "student_name": "冬冬" + str(row["id"])}


def process_batch_items(rows):
    """批量数据处理函数"""
    print(f"执行 process_batch_items（批量处理），处理列表: {rows}")
    return rows


def filter_items(row):
    """过滤函数：只保留id为偶数的记录"""
    print(f"执行 filter_items（过滤处理），检查: {row}")
    return row["class_id"] % 2 == 0


def merge_data():
    """合并函数：返回需要合并的新数据"""
    print("执行 merge_data（合并处理），获取新数据")
    return [{"class_id": 3, "student_name": "新增学生"}]


def join_data():
    """连接数据函数：返回用于连接的数据"""
    print("执行 join_data（连接数据处理），获取连接数据")
    return [
        {"class_id": 1, "teacher": "张老师"},
        {"class_id": 2, "teacher": "李老师"},
        {
            "class_id": 4,
            "teacher": "王老师",
        },  # 这个在左连接中不会出现，但在全连接中会出现
    ]


def join_condition(data_left, data_right):
    """连接条件函数：根据class_id进行连接"""
    print(f"执行 join_condition（连接条件），比较: {data_left} 与 {data_right}")
    return data_left["class_id"] == data_right["class_id"]


def join_data_func():
    """返回(条件函数, 数据)元组的函数"""
    print("执行 join_data_func（单函数连接数据处理）")
    return (join_condition, join_data())


def main():
    """主函数：演示各种操作符的使用"""
    # 创建数据流起始点
    stream_start = Start()

    print("=== 演示全连接功能（传统语法） ===")
    full_join_result = (
        stream_start
        | init_data
        | (DATA > process_single_item)
        | (DATA >> process_batch_items)
        | (DATA - filter_items)
        | (DATA + merge_data)
        | (DATA ** (join_condition, join_data))
    )
    print("全连接最终结果:", full_join_result())

    print("\n=== 演示左连接功能（传统语法） ===")
    left_join_result = (
        stream_start
        | init_data
        | (DATA > process_single_item)
        | (DATA >> process_batch_items)
        | (DATA - filter_items)
        | (DATA + merge_data)
        | (DATA * (join_condition, join_data))
    )
    print("左连接最终结果:", left_join_result())

    print("\n=== 演示全连接功能（单函数语法） ===")
    full_join_single_func_result = (
        stream_start
        | init_data
        | (DATA > process_single_item)
        | (DATA >> process_batch_items)
        | (DATA - filter_items)
        | (DATA + merge_data)
        | (DATA**join_data_func)
    )
    print("全连接（单函数）最终结果:", full_join_single_func_result())

    print("\n=== 演示左连接功能（单函数语法） ===")
    left_join_single_func_result = (
        stream_start
        | init_data
        | (DATA > process_single_item)
        | (DATA >> process_batch_items)
        | (DATA - filter_items)
        | (DATA + merge_data)
        | (DATA * join_data_func)
    )
    print("左连接（单函数）最终结果:", left_join_single_func_result())


if __name__ == "__main__":
    main()
