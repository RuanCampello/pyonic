"""
Database's type system representation.
"""

import struct
from enum import Enum
from typing import Any, Optional


class Type(Enum):
    """Represents the different availables types of the database"""

    INT32 = 1
    """Signed 4-byte integer"""
    FLOAT64 = 2
    """ Double-precision 8-byte floating point number"""
    STRING = 3
    """Variable-length utf-8 character sequence"""
    BOOL = 4
    """Classic Boolean type"""

    @staticmethod
    def from_code(code: int) -> "Type":
        return Type(code)

    def sizeof(self) -> Optional[int]:
        """Returns the size in bytes of a type if its fixed."""
        match self:
            case Type.INT32:
                return 4
            case Type.FLOAT64:
                return 8
            case Type.BOOL:
                return 1
            case _:
                return None

    def encode(self, value: Any) -> bytes:
        """
        Encodes a single value into a respective type format.
        """

        if value is None:
            value = self.null()

        match self:
            case Type.INT32:
                return struct.pack("<i", value)
            case Type.FLOAT64:
                return struct.pack("<d", value)
            case Type.STRING:
                if isinstance(value, str):
                    return value.encode("utf-8")
                return b""
            case Type.BOOL:
                return bytes([1 if value else 0])

    def null(self) -> Any:
        """
        Returns the default null representation of this given type.
        """

        match self:
            case Type.INT32:
                return 0
            case Type.FLOAT64:
                return 0.0
            case Type.STRING:
                return ""
            case Type.BOOL:
                return False
