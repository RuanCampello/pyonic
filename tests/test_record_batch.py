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

    def test_column_length_mismatch(self):
        col1 = Column("id", Type.INT32, [1, 2, 3])
        col2 = Column("name", Type.STRING, ["Alice", "Bob"])

        with self.assertRaises(ValueError) as context:
            RecordBatch([col1, col2])

        self.assertIn("inconsistent length", str(context.exception))

    def test_encoding_binary_structure(self):
        col1 = Column("id", Type.INT32, [1, 2])
        col2 = Column("flag", Type.BOOL, [True, False])

        batch = RecordBatch([col1, col2])
        encoded = batch.encode()

        self.assertIsInstance(encoded, bytes)
        self.assertGreater(len(encoded), 0)

        # header: 2 columns (u16), 2 rows (u32)
        self.assertEqual(encoded[0:2], b"\x02\x00")
        self.assertEqual(encoded[2:6], b"\x02\x00\x00\x00")

    def test_utf8_column(self):
        col = Column("words", Type.STRING, ["hi", "hello", "hallo"])

        batch = RecordBatch([col])
        encoded = batch.encode()

        self.assertIsInstance(encoded, bytes)
        self.assertGreater(len(encoded), 0)


if __name__ == "__main__":
    unittest.main()
