"""
Record Batch groups multiple columns into a single batch of row-aligned
data across the columnar storage.
"""

from typing import List, Self, Tuple
from core.column import Column
from core.types import Type


class RecordBatch:
    """
    Represents a collection of columns sharing the same number of rows.
    """

    def __init__(self, columns: List[Column]) -> None:
        if not columns:
            raise ValueError("RecordBatch must have at least one column")

        length = len(columns[0].values)
        for column in columns:
            if len(column.values) != length:
                raise ValueError(f"Column {column.name} has inconsistent length")

        self.columns = columns
        self.__num_rows = length

    @property
    def num_rows(self) -> int:
        """Returns the number of rows in the batch"""
        return self.__num_rows

    @property
    def num_columns(self) -> int:
        """Returns the number of columns in the batch"""
        return len(self.columns)

    def schema(self) -> List[Tuple[str, Type]]:
        """
        Returns the schema as a list of (column name, type) pairs.
        """

        return [(column.name, column.dtype) for column in self.columns]

    def encode(self) -> bytes:
        """
        Serialises the record batch into binary format.
        This uses little endian notation.
        """

        import struct

        encoded = bytearray()

        # header
        encoded.extend(struct.pack("<H", self.num_columns))  # u16
        encoded.extend(struct.pack("<I", self.num_rows))  # u32

        # schema
        for column in self.columns:
            name = column.name.encode("utf-8")
            name_len = len(name)

            encoded.append(name_len)  # u8 length
            encoded.extend(name)
            encoded.append(column.dtype.value)  # u8 type enum representation
            encoded.append(1 if column.nullable else 0)  # u8 nullable

        # buffers
        for column in self.columns:
            buff = column.encode_buffers()

            # values length + bytes
            encoded.extend(struct.pack("<I", len(buff.values)))
            encoded.extend(buff.values)

            if buff.offsets:
                encoded.extend(struct.pack("<I", len(buff.offsets)))
                encoded.extend(buff.offsets)
            else:
                # we don't have any offset in that case
                encoded.extend(struct.pack("<I", 0))

            if buff.nulls:
                encoded.extend(struct.pack("<I", len(buff.nulls)))
                encoded.extend(buff.nulls)
            else:
                # all values are not nullable
                encoded.extend(struct.pack("<I", 0))

        return bytes(encoded)

    @staticmethod
    def decode(buffer: bytes) -> "RecordBatch":
        """
        Decodes a RecordBatch from a binary buffer.
        """

        from io import BytesIO

        stream = BytesIO(buffer)
        columns = []

        while stream.tell() < len(buffer):
            name_len = int.from_bytes(stream.read(2), "little")
            name = stream.read(name_len).decode("utf-8")

            type_b = stream.read(1)[0]
            nullable = bool(stream.read(1)[0])
            dtype = Type.from_code(type_b)

            n_values = int.from_bytes(stream.read(4), "little")

            bitmap_size = (n_values + 7) // 8
            bitmap = stream.read(bitmap_size)
            nulls = RecordBatch.__unpack_bitmap(bitmap, n_values)

            # calcule offsets
            if dtype == Type.STRING:
                offsets = [
                    int.from_bytes(stream.read(4), "little")
                    for _ in range(n_values + 1)
                ]
            else:
                offsets = None

        return RecordBatch(columns)

    @staticmethod
    def __unpack_bitmap(bitmap: bytes, size: int) -> list[bool]:
        return [bool((byte >> idx) & 1) for byte in bitmap for idx in range(8)][:size]
