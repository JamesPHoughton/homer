import unittest

import matplotlib
matplotlib.use('Agg')
import matplotlib.pylab as plt
import dask.dataframe as dd
import pandas as pd


class TestCreation(unittest.TestCase):
    def test_compute_tree(self):
        from homer.homer import tree

        clusters = dd.read_hdf('resources/MC_gen_clusters_*.hdf', '/clusters')

        relations = dd.read_hdf('resources/MC_gen_relations_*.hdf', '/relations')

        transitions = pd.read_pickle('resources/MC_gen_transitions_list.pickle')

        tree.compute_tree(clusters, relations, transitions,
                          tree_filename='../working/MC_gen_tree.json.gz')



class TestDrawing(unittest.TestCase):
    def test_compute_size_leaf(self):
        from homer import homer
        collection = homer.Homer(tree_filename='resources/tree.json.gz')
        node = collection.tree.find('peace')
        node.compute_size()
        self.assertIsNotNone(node.width)
        self.assertIsNotNone(node.width)
        self.assertGreater(node.width, node.height)

    def test_compute_multiple_leaves(self):
        from homer import homer
        collection = homer.Homer(tree_filename='resources/tree.json.gz')
        collection.tree.find('peace').compute_size()
        collection.tree.find('pray').compute_size()
        collection.tree.find('live').compute_size()

    def test_compute_size_cluster(self):
        from homer import homer
        collection = homer.Homer(tree_filename='resources/tree.json.gz')
        node = collection.tree.find('100')
        node.compute_size()
        self.assertIsNotNone(node.width)
        self.assertIsNotNone(node.width)

    def test_draw_cluster_cluster(self):
        from homer import homer
        collection = homer.Homer(tree_filename='resources/tree.json.gz')
        node = collection.tree.find('100')
        node.compute_size()
        fig = plt.figure()
        ax = plt.gca()
        node.draw(ax, 100, 100)
        repr(node)



