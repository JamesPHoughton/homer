import unittest

import matplotlib
#matplotlib.use('tkagg')
matplotlib.use('Agg')
import matplotlib.pylab as plt


class TestTree(unittest.TestCase):
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



