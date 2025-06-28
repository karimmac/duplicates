import unittest
import shutil
import tempfile
from pathlib import Path

from duplicates.dupe_finder import DupeFinder


class TestDupeFinder(unittest.TestCase):
    def setUp(self):
        # Create a temporary directory for test files
        self.test_dir = tempfile.mkdtemp()
        self.test_path = Path(self.test_dir)
        # Create duplicate files
        (self.test_path / "file1.txt").write_text("duplicate content")
        (self.test_path / "file2.txt").write_text("duplicate content")
        # Create a unique file
        (self.test_path / "unique.txt").write_text("unique content")
        # Create another set of duplicates
        (self.test_path / "dupeA").write_text("abc")
        (self.test_path / "dupeB").write_text("abc")
        # Create a subdirectory with a duplicate
        subdir = self.test_path / "subdir"
        subdir.mkdir()
        (subdir / "file3.txt").write_text("duplicate content")

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def test_find_dupes(self):
        finder = DupeFinder()
        dupes = finder.find_dupes([str(self.test_path)])
        # Flatten and sort for easier assertions
        dupe_sets = [set(map(Path, group)) for group in dupes]
        # There should be two sets of duplicates
        self.assertEqual(len(dupe_sets), 2)
        # Check that all expected duplicates are found
        expected1 = {
            self.test_path / "file1.txt",
            self.test_path / "file2.txt",
            self.test_path / "subdir" / "file3.txt",
        }
        expected2 = {self.test_path / "dupeA", self.test_path / "dupeB"}
        self.assertTrue(any(expected1 == s for s in dupe_sets))
        self.assertTrue(any(expected2 == s for s in dupe_sets))

    def test_no_false_positives(self):
        finder = DupeFinder()
        dupes = finder.find_dupes([str(self.test_path)])
        all_dupes = set(f for group in dupes for f in group)
        # unique.txt should not be in any dupe set
        self.assertNotIn(str(self.test_path / "unique.txt"), all_dupes)


if __name__ == "__main__":
    unittest.main()
