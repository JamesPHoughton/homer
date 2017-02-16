import unittest
import os
import glob
import pandas as pd

class TestFileParsing(unittest.TestCase):
    def test_get_word_cooccurrences(self):
        """basic functionality """
        from homer.homer.parser import get_message_cooccurences

        with open('resources/testfile.txt') as f:
            msg = f.readline()

        sets = get_message_cooccurences(msg, languages=['en'])
        self.assertIsInstance(sets, list)
        self.assertIsInstance(sets[0], tuple)
        self.assertEqual(len(sets[0]), 3)

    def test_parse_tw_file(self):
        """basic functionality """
        from homer.homer.parser import build_weighted_edgelist_db

        for file in glob.glob('../working/test/*'):
            os.remove(file)

        output_files = build_weighted_edgelist_db(
            tw_file_globstring='resources/small_tw_file*.gz',
            intermediate_files_globstring='../working/test/small_itermediates*.csv',
            output_files_globstring='../working/test/small*.csv',
            languages=['en'],
            dates=[20150615],
            threshold=1,
            hashtags_only=False
        )
        self.assertGreater(len(output_files), 0)
        self.assertTrue(os.path.exists('../working/test/small20150615.csv'))
        df = pd.read_csv(output_files[0])
        self.assertGreater(len(df), 2)

        #for file in glob.glob('../working/test/*'):
        #    os.remove(file)

    def test_parse_hashtags_only(self):
        """basic functionality """
        from homer.homer.parser import build_weighted_edgelist_db

        for file in glob.glob('../working/test/*'):
            os.remove(file)

        output_files = build_weighted_edgelist_db(
            tw_file_globstring='resources/small_tw_file*.gz',
            intermediate_files_globstring='../working/test/small_itermediates*.csv',
            output_files_globstring='../working/test/small*.csv',
            languages=['en'],
            dates=[20150615],
            threshold=1,
            hashtags_only=True
        )
        self.assertGreater(len(output_files), 0)
        self.assertTrue(os.path.exists('../working/test/small20150615.csv'))

        df = pd.read_csv(output_files[0])
        self.assertGreater(len(df), 2)

        for file in glob.glob('../working/test/*'):
            os.remove(file)
