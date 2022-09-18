#!/usr/bin/env python3

"""
Find and manage duplicate files.
"""

import argparse
import csv
import functools
import hashlib
import json
import logging
import re
import sys
from enum import IntEnum, auto
from pathlib import Path

LOGGER = logging.getLogger(__file__)
logging.basicConfig(level=logging.DEBUG)


def _output_dupes_json(dupes: list, out_stream):
    json.dump(dupes, out_stream, indent=4)


def _output_dupes_csv(dupes: list, out_stream):
    dupe_lengths = list(map(len, dupes))
    dupes_with_counts = [[i[0]] + i[1] for i in list(zip(dupe_lengths, dupes))]
    header = ['Count'] + ['Path'] * max(dupe_lengths)
    csv_writer = csv.writer(out_stream)
    csv_writer.writerow(header)
    csv_writer.writerows(dupes_with_counts)


def _output_plain(dupes: list, out_stream):
    for i in dupes:
        out_stream.write(' '.join(i) + '\n')


def _output_dupes(dupes: list, out_file: Path, out_type: str):
    out_fn = {
        'CSV': _output_dupes_csv,
        'JSON': _output_dupes_json,
        'PLAIN': _output_plain,
    }
    if out_file:
        with out_file.open('w') as dupes_file:
            out_fn[out_type](dupes, dupes_file)
    else:
        out_fn[out_type](dupes, sys.stdout)


def _read_dupes_csv(in_file: Path):
    with in_file.open('r') as dupes_file:
        return [i[1:] for i in csv.reader(dupes_file)][1:]


def _read_dupes_json(in_file: Path):
    with in_file.open('r') as dupes_file:
        return json.load(dupes_file)


def _read_dupes(in_file: Path, in_type: str):
    in_fn = {
        'CSV': _read_dupes_csv,
        'JSON': _read_dupes_json,
    }
    return in_fn[in_type](in_file)


MIN_SIZE = 1024
MAX_SIZE = -1


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
        return self.value if self.value == FileMetric.MAX else FileMetric(self.value + 1)

    def prev(self):
        """
        Get the previous enum value before this one.
        prev(MIN) => MIN
        """
        return self.value if self.value == FileMetric.MIN else FileMetric(self.value - 1)


def _file_size(file: Path):
    return file.stat(follow_symlinks=False).st_size


def _calc_md5_hash(chunk_size: int, file: Path):
    with file.open('rb') as in_file:
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


metrics = {
    FileMetric.SIZE: _file_size,
    FileMetric.HASH_1K: functools.partial(_md5_hash, MIN_SIZE),
    FileMetric.HASH_MD5: functools.partial(_md5_hash, MAX_SIZE),
}


class DupeFinder():
    """
    Efficiently find duplicate files within a directory, comparing first by file-size,
    then by first 1024-bytes' hash, and then by full MD5 hash, as necessary.
    MD5 is apparently good enough for most comparisons (1 accidental collision every 10^29 or so).
    """

    def __init__(self):
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
            for i in search_dir.iterdir():
                if i.is_symlink():
                    continue

                if i.is_dir():
                    search_dirs.append(i)
                else:
                    self._lookup_dupes(i)

    def find_dupes(self, search_dir: str):
        """
        Find duplicate files within our defined search directories.
        """
        search_dirs = [Path(search_dir)]

        for path in search_dirs:
            assert path.is_dir() and not path.is_symlink(), f'{path} must be a non-symlink directory'

        self._process_dupes(search_dirs)
        dupes_map = self.file_map.get(FileMetric.MAX - 1, {})
        return [[str(i) for i in v] for (_, v) in dupes_map.items()]


def _filter(dupes: list, pattern: str):
    filtered = []
    for row in dupes:
        filtered_row = [i for i in row if re.search(pattern, i)]
        if filtered_row:
            filtered.append(filtered_row)

    return filtered


def _parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--search-dir', '-d', help='Directory to search for duplicates')
    parser.add_argument('--in-file', '-i', help='Input file path', default=None)
    parser.add_argument('--in-type', '-it', help='Input file type (default: JSON)',
                        choices=['CSV', 'JSON'], default='JSON')
    parser.add_argument('--out-file', '-o', help='Output file path', default=None)
    parser.add_argument('--out-type', '-ot', help='Output file type (default: PLAIN)',
                        choices=['CSV', 'JSON', 'PLAIN'], default='PLAIN')
    parser.add_argument('--filter-pattern', '-f', help='Filter pattern regex', default=None)
    return parser.parse_args()


def main():
    """
    Find and manage duplicate files.
    """
    args = _parse_args()

    out_file = None
    if args.out_file:
        out_file = Path(args.out_file)
        assert not out_file.exists(), f'{out_file} already exists'

    dupes = None
    if args.in_file:
        dupes = _read_dupes(Path(args.in_file), args.in_type)
    else:
        dupes = DupeFinder().find_dupes(args.search_dir)

    # Inefficient. But there are other inefficiencies: let's see if this is good enough.
    filtered_dupes = _filter(dupes, args.filter_pattern) if args.filter_pattern else dupes

    _output_dupes(filtered_dupes, out_file, args.out_type)


if __name__ == '__main__':
    main()
