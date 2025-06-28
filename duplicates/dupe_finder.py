import functools
import hashlib
from pathlib import Path
from duplicates.file_metrics import FileMetric
from duplicates.main import LOGGER


def _file_size(file: Path):
    return file.stat(follow_symlinks=False).st_size


def _calc_md5_hash(chunk_size: int, file: Path):
    with file.open("rb") as in_file:
        return hashlib.md5(in_file.read(chunk_size)).hexdigest()


md5_cache = {}


def _md5_hash(chunk_size: int, file: Path):
    if file in md5_cache:
        return md5_cache[file]

    if chunk_size == -1 or chunk_size >= file.stat().st_size:
        md5_hash = _calc_md5_hash(-1, file)
        md5_cache[file] = md5_hash
        return md5_hash

    return _calc_md5_hash(chunk_size, file)


MIN_SIZE = 1024
MAX_SIZE = -1


metrics = {
    FileMetric.SIZE: _file_size,
    FileMetric.HASH_1K: functools.partial(_md5_hash, MIN_SIZE),
    FileMetric.HASH_MD5: functools.partial(_md5_hash, MAX_SIZE),
}


class DupeFinder:
    """
    Efficiently find duplicate files within a directory, comparing first by file-size,
    then by first 1024-bytes' hash, and then by full MD5 hash, as necessary.
    MD5 is apparently good enough for most comparisons (1 accidental collision every 10^29 or so).
    """

    def __init__(self):
        # A map of FileMetric (size, hash etc.) to lists of matching files.
        self.file_map = {}

    def _insert_into_metric_map(self, metric: FileMetric, measure, file: Path):
        if metric not in self.file_map:
            self.file_map[metric] = {}

        metric_map = self.file_map[metric]
        if measure not in metric_map:
            metric_map[measure] = []

        similar_files = metric_map[measure]
        similar_files.append(file)

        return similar_files

    def _lookup_dupes(self, file: Path, metric: FileMetric = FileMetric.MIN):
        if file.is_symlink():
            LOGGER.warning("Ignoring - symlink: %s", file)
            return

        if not file.is_file():
            LOGGER.info("Ignoring - not a file: %s", file)
            return

        measure = metrics[metric](file)
        similar_files = self._insert_into_metric_map(metric, measure, file)

        if len(similar_files) > 1:
            if metric != FileMetric.MAX:
                next_metric = metric.next()
                if len(similar_files) == 2:
                    self._lookup_dupes(similar_files[0], next_metric)
                self._lookup_dupes(file, next_metric)

    def _process_dupes(self, search_dirs: list[Path]):
        for search_dir in search_dirs:
            if search_dir.is_symlink():
                LOGGER.warning("Ignoring - directory symlink: %s", search_dir)
                continue

            for i in search_dir.iterdir():
                if i.is_dir():
                    search_dirs.append(i)
                else:
                    self._lookup_dupes(i)

    def find_dupes(self, search_dirs: list[str]):
        """
        Find all duplicate files across the given search directories.
        """
        search_dirs = [Path(i) for i in search_dirs]

        for path in search_dirs:
            assert (
                path.is_dir() and not path.is_symlink()
            ), f"{path} must be a non-symlink directory"

        self._process_dupes(search_dirs)
        return self.dupes

    def rescan(self, old_dupes: list[list]):
        """
        Rescan an existing set of duplicate files.
        """
        for dupe_list in old_dupes:
            for item, i in enumerate(dupe_list):
                self._lookup_dupes(Path(item))

        return self.dupes

    @property
    def dupes(self):
        """
        Return a list of duplicate-list absolute Paths.
        """
        dupes_map = self.file_map.get(FileMetric.MAX, {})
        return [[str(i.resolve()) for i in v] for (_, v) in dupes_map.items()]
