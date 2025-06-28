from enum import IntEnum, auto


class FileMetric(IntEnum):
    """
    Different metrics for comparing files, ranging from fastest/least accurate to slowest/presumed-exact.
    """

    SIZE = auto()
    HASH_1K = auto()
    HASH_MD5 = auto()
    MAX = HASH_MD5
    MIN = SIZE

    def next(self):
        """
        Get the next enum value after this one.
        next(MAX) => MAX
        """
        return (
            self.value if self.value == FileMetric.MAX else FileMetric(self.value + 1)
        )

    def prev(self):
        """
        Get the previous enum value before this one.
        prev(MIN) => MIN
        """
        return (
            self.value if self.value == FileMetric.MIN else FileMetric(self.value - 1)
        )
