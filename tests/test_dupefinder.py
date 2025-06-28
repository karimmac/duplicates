import shutil
import tempfile
from pathlib import Path
import pytest

from duplicates.dupe_finder import DupeFinder


@pytest.fixture
def temp_dupe_dir():
    test_dir = tempfile.mkdtemp()
    test_path = Path(test_dir)
    # Create duplicate files
    (test_path / "file1.txt").write_text("duplicate content")
    (test_path / "file2.txt").write_text("duplicate content")
    # Create a unique file
    (test_path / "unique.txt").write_text("unique content")
    # Create another set of duplicates
    (test_path / "dupeA").write_text("abc")
    (test_path / "dupeB").write_text("abc")
    # Create a subdirectory with a duplicate
    subdir = test_path / "subdir"
    subdir.mkdir()
    (subdir / "file3.txt").write_text("duplicate content")
    yield test_path
    shutil.rmtree(test_dir)


def test_find_dupes(temp_dupe_dir):
    finder = DupeFinder()
    dupes = finder.find_dupes([str(temp_dupe_dir)])
    dupe_sets = [set(map(Path, group)) for group in dupes]
    assert len(dupe_sets) == 2
    expected1 = {
        temp_dupe_dir / "file1.txt",
        temp_dupe_dir / "file2.txt",
        temp_dupe_dir / "subdir" / "file3.txt",
    }
    expected2 = {temp_dupe_dir / "dupeA", temp_dupe_dir / "dupeB"}
    assert any(expected1 == s for s in dupe_sets)
    assert any(expected2 == s for s in dupe_sets)


def test_no_false_positives(temp_dupe_dir):
    finder = DupeFinder()
    dupes = finder.find_dupes([str(temp_dupe_dir)])
    all_dupes = set(f for group in dupes for f in group)
    assert str(temp_dupe_dir / "unique.txt") not in all_dupes
