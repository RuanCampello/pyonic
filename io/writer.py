from core.record_batch import RecordBatch
import struct

EXTENSION = ".ionic"


class Writer:
    """Writes `RecordBatch`s to a binary file in a length-prefixed format"""

    def __init__(self, path: str) -> None:
        if not path.endswith(EXTENSION):
            path += EXTENSION
        self.file = open(path, "wb")

    def write(self, batch: RecordBatch) -> None:
        encode = batch.encode()
        length = len(encode)

        # format: [u32 length][batch content in bytes]
        self.file.write(struct.pack("<I", length))  # u32
        self.file.write(encode)

    def close(self) -> None:
        self.file.close()
