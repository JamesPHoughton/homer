import unittest
import dask


class TestFileParsing(unittest.TestCase):
    def test_get_word_cooccurrences(self):
        """basic functionality """
        from homer.parser import get_word_cooccurrences

        with open('resources/testfile.txt') as f:
            msg = f.readline()

        sets = get_word_cooccurrences(msg)
        self.assertIsInstance(sets, list)
        self.assertIsInstance(sets[0], tuple)
        self.assertEqual(len(sets[0]), 3)

    def test_parse_tw_file(self):
        """basic functionality """
        from homer.parser import get_weighted_edgelist

        df = get_weighted_edgelist('resources/testfile.txt')
        self.assertIsInstance(df, dask.dataframe.DataFrame)

