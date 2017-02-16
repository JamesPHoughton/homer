import unittest
import dask.dataframe as dd
import pandas as pd
import glob
import os
import shutil


# todo: should find a way to check that any created directories get deleted...


class TestClusterer(unittest.TestCase):
    def test_find_clusters(self):
        from homer.homer import clusterer

        for file in glob.glob('../working/test/*', recursive=True):
            try:
                os.remove(file)
            except PermissionError:
                shutil.rmtree(file)

        w_el_files = glob.glob('resources/MC_synth_data_*.csv')
        intermediate_files_dir = '../working/test/intermediates'
        output_files_directory = '../working/test/MC_clusters'
        dates = list(range(1, 5))
        thresholds = [1]

        cluster_files = clusterer.build_cluster_db(
            weighted_edge_list_files=w_el_files,
            intermediate_files_directory=intermediate_files_dir,
            output_files_directory=output_files_directory,
            dates=dates,
            thresholds=thresholds)



        # uw_el = dd.read_csv('resources/unweighted_edgelist_sample_small.txt', sep=' ')
        # clusters = clusterer.find_clusters(uw_el)
        # self.assertIsInstance(clusters, pd.DataFrame)
        # self.assertIn('k', clusters.columns)
        # self.assertIn('Set', clusters.columns)

    def test_traverse_thresholds(self):
        from homer.homer.clusterer import build_cluster_db
        w_el = dd.read_csv('resources/weighted_edgelist_sample.txt',
                           sep=' ', names=['W1', 'W2', 'Count'])
        w_el['Date'] = 1
        clusters = build_cluster_db(w_el, '../working/test_output_*.csv')
