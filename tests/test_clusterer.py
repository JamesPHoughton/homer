import unittest
import dask.dataframe as dd


class TestClusterer(unittest.TestCase):
    def test_find_cliques(self):
        from homer import clusterer
        uw_el = dd.read_csv('resources/unweighted_edgelist_sample.txt', sep=' ')
        cliques = clusterer.find_cliques(uw_el)
        self.assertIsInstance(cliques, pd.DataFrame)

    def test_read_COS_output_file(self):
        from homer import clusterer
        clusterer.read_COS_output_file(uw_el)
