import unittest

testfile = "testfile.txt"
gz_testfile = "testfile.txt.gz"

class TestFileParsing(unittest.TestCase):
    def test_line_parsing(self):
        from homer.create_weighted_edgelist import get_word_cooccurrences
        with open(testfile) as file:
            lines = file.readlines()
        res = get_word_cooccurrences(lines[3])

    def test_file_parsing(self):
        from homer.create_weighted_edgelist import parse_twitter_json_file
        parse_twitter_json_file(gz_testfile)

    def long_test(self):
        from homer.create_weighted_edgelist import parse_twitter_json_file
        res = parse_twitter_json_file('testfile_1000.txt.gz')
        print 'hi'

    def super_long_test(self):
        from homer.create_weighted_edgelist import parse_twitter_json_file
        res = parse_twitter_json_file('posts_sample_20160101_100206_fz.txt')
        print 'hi'

    def test_collect_results(self):
        from homer.create_weighted_edgelist import collect_results
        res = collect_results(['testfile.txt.gz', 'testfile_1000.txt.gz'])

