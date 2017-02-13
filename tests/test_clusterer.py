import unittest
import dask.dataframe as dd
import pandas as pd

# todo: should find a way to check that any created directories get deleted...


class TestClusterer(unittest.TestCase):
    def test_find_clusters(self):
        from homer.homer import clusterer
        uw_el = dd.read_csv('resources/unweighted_edgelist_sample_small.txt', sep=' ')
        clusters = clusterer.find_clusters(uw_el)
        self.assertIsInstance(clusters, pd.DataFrame)
        self.assertIn('k', clusters.columns)
        self.assertIn('Set', clusters.columns)

    def test_traverse_thresholds(self):
        from homer.homer.clusterer import build_cluster_db
        w_el = dd.read_csv('resources/weighted_edgelist_sample.txt',
                           sep=' ', names=['W1', 'W2', 'Count'])
        w_el['Date'] = 1
        clusters = build_cluster_db(w_el, '../working/test_output_*.csv')



