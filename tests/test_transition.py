import unittest


class testTransition(unittest.TestCase):
    def test_transition(self):
        from homer import homer
        collection = homer.Homer(
            clusters_globstring='resources/MC_gen_clusters_*.hdf',
            transition_clusters_globstring='resources/MC_gen_transitions_*.hdf')

        collection.compute_transition_list('../working/MC_gen_transitions_list.pickle')