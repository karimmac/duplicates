#!/usr/bin/env python3

"""
Find and manage duplicate files.
"""

import argparse
import csv
import json
from pathlib import Path

import find_duplicate_files


def _output_dupes_json(dupes: list, out_file: Path):
    with out_file.open('w') as dupes_file:
        json.dump(dupes, dupes_file)


def _output_dupes_csv(dupes: list, out_file: Path):
    dupe_lengths = list(map(len, dupes))
    dupes_with_counts = [[i[0]] + i[1] for i in list(zip(dupe_lengths, dupes))]
    header = ['Count'] + ['Path'] * max(dupe_lengths)
    with out_file.open('w') as dupes_file:
        csv_writer = csv.writer(dupes_file)
        csv_writer.writerow(header)
        csv_writer.writerows(dupes_with_counts)


def _output_dupes(dupes: list, out_file: Path, out_type: str):
    out_fn = {
        'CSV': _output_dupes_csv,
        'JSON': _output_dupes_json,
    }
    out_fn[out_type](dupes, out_file)


def _print_dupes(dupes: list):
    for i in [[f'"{f}"' for f in row] for row in dupes]:
        print(' '.join(i))


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


def _parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--search_dir', '-d', help='Directory to search for duplicates')
    parser.add_argument('--in_file', '-i', help='Input file path', default=None)
    parser.add_argument('--in_type', '-it', help='Input file type (default: JSON)',
                        choices=['CSV', 'JSON'], default='JSON')
    parser.add_argument('--out_file', '-o', help='Output file path', default=None)
    parser.add_argument('--out_type', '-t', help='Output file type (default: CSV)',
                        choices=['CSV', 'JSON'], default='CSV')
    return parser.parse_args()


def _find_dupes(search_dir_path: str):
    search_dir = Path(search_dir_path)
    if not search_dir.is_dir():
        raise RuntimeError(f'{search_dir} is not a directory')

    return find_duplicate_files.find_duplicate_files(search_dir, chunks=2)


def main():
    """
    Find and manage duplicate files.
    """
    args = _parse_args()

    out_file = Path(args.out_file) if args.out_file else None
    if out_file and out_file.exists():
        raise RuntimeError(f'{out_file} already exists')

    dupes = None
    if args.in_file:
        dupes = _read_dupes(Path(args.in_file), args.in_type)
    else:
        dupes = _find_dupes(args.search_dir)

    if out_file:
        _output_dupes(dupes, out_file, args.out_type)
    else:
        _print_dupes(dupes)


if __name__ == '__main__':
    main()
