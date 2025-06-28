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
from pathlib import Path
from typing import Iterable

from duplicates.dupe_finder import DupeFinder
from duplicates.file_metrics import FileMetric

LOGGER = logging.getLogger(__file__)
logging.basicConfig()


def _output_dupes_json(dupes: Iterable, out_stream):
    json.dump(list(dupes), out_stream, indent=4)


def _output_dupes_csv(dupes: Iterable, out_stream):
    dupes_list = list(dupes)
    dupe_lengths = list(map(len, dupes_list))
    dupes_with_counts = [[i[0]] + i[1] for i in list(zip(dupe_lengths, dupes_list))]
    header = ["Count"] + ["Path"] * max(dupe_lengths)
    csv_writer = csv.writer(out_stream)
    csv_writer.writerow(header)
    csv_writer.writerows(dupes_with_counts)


def _output_plain(dupes: Iterable, out_stream):
    for i in dupes:
        out_stream.write(" ".join(i) + "\n")


def _output_dupes(dupes: list, out_file: Path, out_type: str):
    out_fn = {
        "CSV": _output_dupes_csv,
        "JSON": _output_dupes_json,
        "PLAIN": _output_plain,
    }
    if out_file:
        with out_file.open("w") as dupes_file:
            out_fn[out_type](dupes, dupes_file)
    else:
        out_fn[out_type](dupes, sys.stdout)


def _read_dupes_csv(in_file: Path):
    with in_file.open("r") as dupes_file:
        return [i[1:] for i in csv.reader(dupes_file)][1:]


def _read_dupes_json(in_file: Path):
    with in_file.open("r") as dupes_file:
        return json.load(dupes_file)


def _read_dupes(in_file: Path, in_type: str):
    in_fn = {
        "CSV": _read_dupes_csv,
        "JSON": _read_dupes_json,
    }
    return in_fn[in_type](in_file)


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
    parser.add_argument(
        "--search-dir",
        "-d",
        action="append",
        help="Directory to search for duplicates (multiple entries accepted)",
    )
    parser.add_argument("--in-file", "-i", help="Input file path", default=None)
    parser.add_argument(
        "--in-type",
        "-it",
        help="Input file type (default: JSON)",
        choices=["CSV", "JSON"],
        default="JSON",
    )
    parser.add_argument("--out-file", "-o", help="Output file path", default=None)
    parser.add_argument(
        "--out-type",
        "-ot",
        help="Output file type (default: PLAIN)",
        choices=["CSV", "JSON", "PLAIN"],
        default="PLAIN",
    )
    filter_help = "Filter items by regex pattern. If _all_ duplicates in a set match then the last is not returned, to avoid deleting all copies of a file."
    parser.add_argument("--filter-pattern", "-f", help=filter_help, default=None)
    parser.add_argument(
        "--rescan",
        "-r",
        help="Rescan items from input-file",
        action=argparse.BooleanOptionalAction,
        default=False,
    )
    parser.add_argument(
        "--verbose",
        "-v",
        help="Verbose output - e.g. print each processed file",
        action=argparse.BooleanOptionalAction,
        default=False,
    )

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
    LOGGER.debug("args: %s", args)

    assert not (
        args.search_dir and args.in_file
    ), "Cannot specify both search directories and input file"

    out_file = None
    if args.out_file:
        out_file = Path(args.out_file)
        assert not out_file.exists(), f"{out_file} already exists"

    def resolve_fn(i):
        return i

    dupes = None
    if args.in_file:
        old_dupes = _read_dupes(Path(args.in_file), args.in_type)
        LOGGER.debug("Read %s existing duplicate sets", len(old_dupes))
        if not args.rescan:
            dupes = old_dupes
            resolve_fn = _resolve_to_cwd
        else:
            dupes = DupeFinder().rescan(old_dupes)
    else:
        dupes = DupeFinder().find_dupes(args.search_dir)

    # Inefficient. But there are other inefficiencies: let's see if this is good enough.
    filtered_dupes = (
        _filter(dupes, args.filter_pattern) if args.filter_pattern else dupes
    )
    resolved_dupes = resolve_fn(filtered_dupes)

    _output_dupes(resolved_dupes, out_file, args.out_type)


if __name__ == "__main__":
    main()
