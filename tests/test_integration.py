import unittest
import random
from antchain import Start, DATA, COUNT, SET, NON


def init_user():
    return [
        {"id": 1, "name": "Alice"},
        {"id": 2, "name": "Bob"},
        {"id": 3, "name": "Charlie"},
        {"id": 4, "name": "David"},
    ]


def add_age(row):
    row["age"] = random.randint(18, 65)
    return row


def add_address(row):
    row["address"] = random.choice(["北京", "上海", "广州", "深圳"])
    return row


def filter_adult(row):
    return row["age"] >= 18


def process_batch(rows, stream_size=2):
    for row in rows:
        row["processed"] = True
    return rows


def get_teacher_data(rows, stream_size=2):
    return [
        {"id": 1, "teacher": "Mr. Smith"},
        {"id": 2, "teacher": "Ms. Johnson"},
        {"id": 3, "teacher": "Dr. Brown"},
    ]


def join_func(
    left_key=lambda x: x["id"],
    right_key=lambda x: x["id"],
    left_property="teacher_info",
    one_to_many=False,
):
    pass


class TestIntegration(unittest.TestCase):

    def test_basic_stream_processing(self):
        """测试基本的数据流处理"""
        start = Start()
        chain = (
            start
            | init_user
            | (DATA > add_age)
            | (DATA > add_address)
            | (DATA - filter_adult)
        )

        result = chain()
        self.assertEqual(len(result), 4)
        for item in result:
            self.assertIn("id", item)
            self.assertIn("name", item)
            self.assertIn("age", item)
            self.assertIn("address", item)
            self.assertGreaterEqual(item["age"], 18)

    def test_batch_processing(self):
        """测试批处理功能"""
        start = Start()
        chain = start | init_user | (DATA >> process_batch)

        result = chain()
        self.assertEqual(len(result), 4)
        for item in result:
            self.assertIn("processed", item)
            self.assertTrue(item["processed"])

    def test_merge_operation(self):
        """测试合并操作"""

        def add_extra_user():
            return [{"id": 5, "name": "Eve"}]

        start = Start()
        chain = start | init_user | (DATA + add_extra_user)

        result = chain()
        self.assertEqual(len(result), 5)
        self.assertEqual(result[-1]["name"], "Eve")

    def test_count_operation(self):
        """测试计数操作"""
        start = Start()
        chain = start | init_user | COUNT

        result = chain()
        self.assertEqual(result, 4)

    def test_non_operation(self):
        """测试NON操作"""

        def init_with_none():
            return [1, None, 2, None, 3]

        start = Start()
        chain = start | init_with_none | NON

        result = chain()
        self.assertEqual(result, [1, 2, 3])

    def test_set_operation(self):
        """测试SET操作"""

        def init_with_duplicates():
            return [1, 2, 2, 3, 3, 3]

        start = Start()
        chain = start | init_with_duplicates | SET

        result = chain()
        self.assertEqual(len(result), 3)
        self.assertIn(1, result)
        self.assertIn(2, result)
        self.assertIn(3, result)


if __name__ == "__main__":
    unittest.main()
