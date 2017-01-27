from . import clusterer
from . import parser
from . import relate
from . import tree
import dask.dataframe as dd
import gzip
import json


def new_collection(tw_file_globstring,
                   weighted_edge_list_globstring,
                   clusters_globstring,
                   hashtags_only=False, min_threshold=1):
    """
    Processes twitter JSON data to find clusters, creates a new homer object

    Parameters
    ----------
    tw_file_globstring: raw data source
    weighted_edge_list_globstring:
        This is a globstring to describe where the weigted edgelist will be saved
    clusters_globstring
        where to save info about clusters
    hashtags_only
    min_threshold

    Returns
    -------

    """

    weighted_edge_list = parser.build_weighted_edgelist_db(
        tw_file_globstring=tw_file_globstring,
        output_globstring=weighted_edge_list_globstring,
        hashtags_only=hashtags_only
    )

    clusters = clusterer.build_cluster_db(
        weighted_edge_list,
        output_globstring=clusters_globstring,
        min_threshold=min_threshold
    )

    collection = Homer(weighted_edge_list_globstring,
                       clusters_globstring)

    return collection


class Homer(object):
    """


    This object maintains a connection to the database
    and provides methods for accessing clusters and components

    The object can be created by linking it to an existing store of
    processed data, or by



    """

    def __init__(self,
                 weighted_edge_list_globstring=None,
                 clusters_globstring=None,
                 relations_globstring=None,
                 tree_filename=None):
        """

        Parameters
        ----------
        weighted_edge_list_globstring

        clusters_globstring (optional)

        you need at least one of these to be able to do anything, though.

        """
        if weighted_edge_list_globstring is not None:
            self.weighted_edge_list = dd.read_hdf(weighted_edge_list_globstring,
                                                  '/weighted_edge_list')

        if clusters_globstring is not None:
            self.clusters = dd.read_hdf(clusters_globstring, '/clusters')

        if relations_globstring is not None:
            self.relations = dd.read_hdf(relations_globstring, '/relations')

        if tree_filename is not None:
            self.tree = tree.load_tree(tree_filename)

    def compute_clusters(self, clusters_globstring, min_threshold):
        self.clusters = clusterer.build_cluster_db(
            self.weighted_edge_list,
            output_globstring=clusters_globstring,
            min_threshold=min_threshold
        )

    def compute_relations(self, relations_globstring):
        self.relations = relate.build_relations_db(
            clusters=self.clusters,
            output_globstring=relations_globstring
        )

    def compute_tree(self, tree_filename):
        self.tree = tree.compute_tree(
            clusters=self.clusters,
            relations=self.relations,
            tree_filename=tree_filename
        )

    def get_clusters_by_keyword(self,
                                keywords=None,
                                first_date=None,
                                last_date=None):
        """
        Retrieves clusters containing corresponding any of the given keywords,
        within the specified date range.

        Parameters
        ----------
        keywords: list of strings
            will return messages containing any of the keywords
        first_date
        last_date

        Returns
        -------
        clusters: DataFrame
        """

        date_range = self.clusters[first_date <= self.clusters['Date'] <= last_date]
        selection = date_range[date_range['Set'].apply(
            lambda x: all([kw in x for kw in keywords]))].compute()

        return selection

    def get_child_clusters(self, cluster_ids, generations=1):
        """
        For a given cluster or set of clusters
        Parameters
        ----------
        cluster_ids: list of strings
            list of ids of clusters
        generations

        Returns
        -------

        """
        raise NotImplementedError

    def draw_nested_sets(self, cluster_ids, depth=-1):
        """

        Parameters
        ----------
        cluster_ids
        depth

        Returns
        -------

        """

