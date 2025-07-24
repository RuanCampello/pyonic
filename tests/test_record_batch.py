import unittest

from core.column import Column
from core.record_batch import RecordBatch
from core.types import Type


class TestRecordBatch(unittest.TestCase):
    def test_schema_and_counts(self):
        col_1 = Column("id", Type.INT32, [1, 2, 3])
        col_2 = Column("name", Type.STRING, ["Alice", "Bob", "Carol"])

        batch = RecordBatch([col_1, col_2])

        self.assertEqual(batch.num_columns, 2)
        self.assertEqual(batch.num_rows, 3)
        self.assertEqual(batch.schema(), [("id", Type.INT32), ("name", Type.STRING)])


if __name__ == "__main__":
    unittest.main()
