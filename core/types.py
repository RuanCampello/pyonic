"""
Database's type system representation.
"""

from enum import Enum
from typing import Optional


class Type(Enum):
    INT32 = 1
    FLOAT64 = 2
    STRING = 3
    BOOL = 4

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
