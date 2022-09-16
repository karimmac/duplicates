#!/usr/bin/env python3

import argparse
import csv
from pathlib import Path

import find_duplicate_files


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--search_dir', '-d', help='Directory to search for duplicates')
    args = parser.parse_args()

    dupes = find_duplicate_files.find_duplicate_files(args.search_dir, chunks=2)
    dupe_lengths = list(map(len, dupes))
    dupes_with_counts = [[i[0]] + i[1] for i in list(zip(dupe_lengths, dupes))]
    header = ['Count'] + ['Path'] * max(dupe_lengths)
    with Path('dupes.csv').open('w') as dupes_file:
        csv_writer = csv.writer(dupes_file)
        csv_writer.writerow(header)
        csv_writer.writerows(dupes_with_counts)


if __name__ == '__main__':
    main()
