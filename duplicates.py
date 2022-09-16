#!/usr/bin/env python3

import argparse
import csv
from pathlib import Path

import find_duplicate_files


def _output_dupes_csv(dupes, out_file):
    dupe_lengths = list(map(len, dupes))
    dupes_with_counts = [[i[0]] + i[1] for i in list(zip(dupe_lengths, dupes))]
    header = ['Count'] + ['Path'] * max(dupe_lengths)
    with out_file.open('w') as dupes_file:
        csv_writer = csv.writer(dupes_file)
        csv_writer.writerow(header)
        csv_writer.writerows(dupes_with_counts)


def _output_dupes(dupes, out_file, out_type):
    out_fn = {
        'CSV': _output_dupes_csv
    }
    out_fn[out_type](dupes, out_file)


def _print_dupes(dupes):
    for i in [[f'"{f}"' for f in row] for row in dupes]:
        print(' '.join(i))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--search_dir', '-d', help='Directory to search for duplicates', required=True)
    parser.add_argument('--out_file', '-o', help='Output file path', default=None)
    parser.add_argument('--out_type', '-t', help='Output file type (default: CSV)', choices=['CSV'], default='CSV')
    args = parser.parse_args()

    out_file = Path(args.out_file) if args.out_file else None
    if out_file and out_file.exists():
        raise RuntimeError(f'{out_file} already exists')

    dupes = find_duplicate_files.find_duplicate_files(args.search_dir, chunks=2)

    if out_file:
        _output_dupes(dupes, out_file, args.out_type)
    else:
        _print_dupes(dupes)


if __name__ == '__main__':
    main()
