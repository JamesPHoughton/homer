import unittest
import glob
import os


class TestHomer(unittest.TestCase):
    def test_multi_files(self):
        import homer
        files = glob.glob('resources/testfile*.gz')
        self.assertGreater(len(files), 1)
        w_el = homer.get_weighted_edgelist(files)

        clusters = homer.find_clusters_for_any_threshold(w_el)
        head = clusters.head()
        repr(head)


class TestNewCollection(unittest.TestCase):
    def test_builds(self):
        for f in glob.glob('../working/*'):
            os.remove(f)

        from homer.homer import new_collection
        collection = new_collection(
            tw_file_globstring='resources/testfile*.gz',
            weighted_edge_list_globstring='../working/weighted_edgelists_*.hdf',
            clusters_globstring='../working/clusters_*.hdf',
            min_threshold=3)

        repr(collection)

    def test_get_by_keyword(self):
        from homer.homer import Homer
        collection = Homer(weighted_edge_list_globstring='../working/weighted_edgelists_*.hdf',
                           clusters_globstring='../working/clusters_*.hdf')

        collection.get_clusters_by_keyword()