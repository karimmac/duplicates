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
from typing import Iterable

LOGGER = logging.getLogger(__file__)
logging.basicConfig()


def _output_dupes_json(dupes: Iterable, out_stream):
    json.dump(list(dupes), out_stream, indent=4)


def _output_dupes_csv(dupes: Iterable, out_stream):
    dupes_list = list(dupes)
    dupe_lengths = list(map(len, dupes_list))
    dupes_with_counts = [[i[0]] + i[1] for i in list(zip(dupe_lengths, dupes_list))]
    header = ['Count'] + ['Path'] * max(dupe_lengths)
    csv_writer = csv.writer(out_stream)
    csv_writer.writerow(header)
    csv_writer.writerows(dupes_with_counts)


def _output_plain(dupes: Iterable, out_stream):
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
        if file.is_symlink():
            LOGGER.warning('Ignoring - symlink: %s', file)
            return

        if not file.is_file():
            LOGGER.info('Ignoring - not a file: %s', file)
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
                LOGGER.warning('Ignoring - directory symlink: %s', search_dir)
                continue

            for i in search_dir.iterdir():
                if i.is_dir():
                    search_dirs.append(i)
                else:
                    self._lookup_dupes(i)

    def find_dupes(self, search_dirs: list[str]):
        """
        Find duplicate files within our defined search directories.
        """
        search_dirs = [Path(i) for i in search_dirs]

        for path in search_dirs:
            assert path.is_dir() and not path.is_symlink(), f'{path} must be a non-symlink directory'

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


def _filter(dupes: list, pattern: str):
    filtered = []
    for dupe_list in dupes:
        # Should never happen!
        if len(dupe_list) < 2:
            continue

        filtered_dupe_list = [i for i in dupe_list if re.search(pattern, i)]
        if filtered_dupe_list:
            # By default if all items match then keep (do not return) the last item,
            # to reduce the odds of inadvertently deleting all copies of a file.
            if len(filtered_dupe_list) == len(dupe_list):
                filtered_dupe_list.pop()

            filtered.append(filtered_dupe_list)

    return filtered


def _resolve_path_to_dir(root: str, path: str):
    return path if Path(path).is_absolute() else Path(root) / path


def _resolve_to_cwd(dupes: list):
    """
    Attempt to resolve the list of dupes relative to the current working directory.
    Any absolute paths are left untouched.
    This does _not_ do 'true' resolve in the same way as Path.resolve() et al,
    as we wish to leave any symlinks untouched.
    """
    cwd = Path.cwd()
    resolved = []
    for dupe_list in dupes:
        resolved_dupe_list = [str(_resolve_path_to_dir(cwd, i)) for i in dupe_list]
        if resolved_dupe_list:
            resolved.append(resolved_dupe_list)

    return resolved


def _parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--search-dir', '-d', action='append',
                        help='Directory to search for duplicates (multiple entries accepted)')
    parser.add_argument('--in-file', '-i', help='Input file path', default=None)
    parser.add_argument('--in-type', '-it', help='Input file type (default: JSON)',
                        choices=['CSV', 'JSON'], default='JSON')
    parser.add_argument('--out-file', '-o', help='Output file path', default=None)
    parser.add_argument('--out-type', '-ot', help='Output file type (default: PLAIN)',
                        choices=['CSV', 'JSON', 'PLAIN'], default='PLAIN')
    filter_help = 'Filter items by regex pattern. If _all_ duplicates in a set match then the last is not returned, to avoid deleting all copies of a file.'
    parser.add_argument('--filter-pattern', '-f', help=filter_help, default=None)
    parser.add_argument('--rescan', '-r', help='Rescan items from input-file',
                        action=argparse.BooleanOptionalAction, default=False)
    parser.add_argument('--verbose', '-v', help='Verbose output - e.g. print each processed file',
                        action=argparse.BooleanOptionalAction, default=False)

    args = parser.parse_args()
    if not (args.search_dir or args.in_file):
        parser.print_help()
        sys.exit(1)

    return args


def main():
    """
    Find and manage duplicate files.
    """
    args = _parse_args()
    LOGGER.setLevel(logging.DEBUG if args.verbose else logging.INFO)
    LOGGER.debug('args: %s', args)

    assert not (args.search_dir and args.in_file), 'Cannot specify both search directories and input file'

    out_file = None
    if args.out_file:
        out_file = Path(args.out_file)
        assert not out_file.exists(), f'{out_file} already exists'

    def resolve_fn(i):
        return i

    dupes = None
    if args.in_file:
        old_dupes = _read_dupes(Path(args.in_file), args.in_type)
        LOGGER.debug('Read %s existing duplicate sets', len(old_dupes))
        if not args.rescan:
            dupes = old_dupes
            resolve_fn = _resolve_to_cwd
        else:
            dupes = DupeFinder().rescan(old_dupes)
    else:
        dupes = DupeFinder().find_dupes(args.search_dir)

    # Inefficient. But there are other inefficiencies: let's see if this is good enough.
    filtered_dupes = _filter(dupes, args.filter_pattern) if args.filter_pattern else dupes
    resolved_dupes = resolve_fn(filtered_dupes)

    _output_dupes(resolved_dupes, out_file, args.out_type)


if __name__ == '__main__':
    main()
