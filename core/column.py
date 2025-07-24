"""
Columns representation. Responsable of encoding data buffers,
null bitmap and string offsets.
"""

import struct

from dataclasses import dataclass
from typing import List, Optional, Union
from core.types import Type


@dataclass
class ColumnBuffers:
    """
    Encapsulates the encoded binary buffers for a column.
    """

    values: bytes
    nulls: Optional[bytes] = None
    offsets: Optional[bytes] = None


class Column:
    """
    Represents a single column of data in a columnar table.
    """

    def __init__(
        self,
        name: str,
        dtype: Type,
        values: List[Union[int, float, str, bool, None]],
        nullable: bool = False,
    ) -> None:
        self.name = name
        self.dtype = dtype
        self.values = values
        self.nullable = nullable

        self.null_bitmap = self.__build_null_bitmap() if nullable else None
        self.offsets = self.__build_offsets() if dtype == Type.STRING else None

    def encode_buffers(self) -> ColumnBuffers:
        """
        Encodes this column into raw bytes buffers, ready for serialisation.
        """
        buff = bytearray()

        for value in self.values:
            buff.extend(self.dtype.encode(value))

        offsets = (
            b"".join(struct.pack("<I", off) for off in self.offsets)
            if self.offsets
            else None
        )

        return ColumnBuffers(
            values=bytes(buff), nulls=self.null_bitmap, offsets=offsets
        )

    def __build_null_bitmap(self) -> bytes:
        """
        Builds a compact null bitmap, 1 bit per row, packed in bytes.
        A bit is 0 if the value is `None`, 1 otherwise.
        """

        bits = 0
        results = bytearray()
        count = 0

        for idx, value in enumerate(self.values):
            if value is not None:
                bits |= 1 << (idx % 8)
            count += 1

            if count == 8:
                results.append(bits)
                bits, count = 0, 0

        if count > 0:
            results.append(bits)

        return bytes(results)

    def __build_offsets(self) -> List[int]:
        """
        Builds an offset buffer for UTF8 (variable-length) strings.
        Each offset marks the start byte position of a string.
        The list ends with the total byte size.
        """

        assert self.dtype == Type.STRING, "Offsets only apply to UTF8"

        offsets = [0]
        cursor = 0

        for value in self.values:
            if value is None:
                encoded = b""
            elif isinstance(value, str):
                encoded = value.encode("utf-8")
            else:
                raise TypeError(f"Expected str or None, but got {type(value).__name__}")

            cursor += len(encoded)
            offsets.append(cursor)

        return offsets

