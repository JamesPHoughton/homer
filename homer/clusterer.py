"""
clusterer.py

functions in this file take network structure expressed as
edges and translate it to network structure expressed as nested
clusters

"""

import subprocess
import glob
import pandas as pd
import os
import shutil
import tempfile
import dask.dataframe as dd
from dask import delayed
import numpy as np

TOOLS_DIR = os.path.dirname(os.path.realpath(__file__))


class TemporaryDirectory(object):
    def __enter__(self):
        self.name = tempfile.mkdtemp()
        return self.name

    def __exit__(self, exc_type, exc_value, traceback):
        shutil.rmtree(self.name)


def find_clusters(unweighted_edge_list):
    """
    Uses COSparallel to identify new clusters from an unweighted edgelist.

    Parameters
    ----------
    unweighted_edge_list: dask (or pandas) dataframe with columns 'W1' and 'W2' only

    Returns
    -------
    pandas DataFrame
    """
    # create a working directory
    with TemporaryDirectory() as dir_name:

        if isinstance(unweighted_edge_list, dd.DataFrame):
            # repartition to be in one partition
            uw_el = unweighted_edge_list.repartition(npartitions=1)

            # write the unweighted edgelist to a file
            glob_filename = 'unweighted_edge_list.*.txt'
            edgelist_filename = glob_filename.replace('*', '0')

            uw_el.to_csv(dir_name + '/' + glob_filename, sep=' ',
                         index=False, header=False, encoding='utf-8')

        elif isinstance(unweighted_edge_list, pd.DataFrame):
            uw_el = unweighted_edge_list

            edgelist_filename = 'unweighted_edge_list.txt'
            uw_el.to_csv(dir_name + '/' + edgelist_filename, sep=' ',
                         index=False, header=False, encoding='utf-8')

        else:
            return pd.DataFrame(columns=['Set', 'k'])

        # run .mcliques, then for each output file, run cosparallel
        cmds = ['cd %s' % dir_name,
                '%s/./maximal_cliques %s' % (
                    TOOLS_DIR.replace(' ', '\ '),  # there must be better ways to escape spaces
                    edgelist_filename.replace(' ', '\ ')),
                '%s/./cos %s.mcliques' % (TOOLS_DIR.replace(' ', '\ '),
                                          edgelist_filename.replace(' ', '\ '))]

        response = subprocess.check_output(r'; '.join(cmds), shell=True)

        # read the mapping file created by 'maximal_cliques'
        map_filename = glob.glob(dir_name + '/*.map')

        mapping = pd.read_csv(map_filename[0], sep=' ', header=None,
                              names=['word', 'number'],
                              index_col='number')

        # read the community files
        community_files = glob.glob(dir_name + '/[0-9]*_communities.txt')

        if len(community_files) > 0:
            collector = []

            for infile in community_files:
                k = int(os.path.basename(infile).rstrip('_communities.txt'))
                clusters = lazy_read_output(infile, mapping)
                clusters = clusters.assign(k=k)
                collector.append(clusters)

            template = pd.DataFrame([{'Set': 'Toad Bug', 'k': 5}],
                                    columns=['Set', 'k'])

            clusters_df = dd.from_delayed(collector, meta=template).compute()
        else:
            clusters_df = pd.DataFrame(columns=['Set', 'k'])

    return clusters_df


def read_COS_output_file(infile_name, mapping):
    """
    Reads an output of the COS program (there are multiple)
    and returns a dictionary with keys being the integer cluster name,
    and elements being a set of the keywords in that cluster

    Parameters
    ----------
    infile_name: unicode string
        full path to the output file

    Returns
    -------
    clusters: dictionary

    """
    clusters = dict()
    with open(infile_name, 'r') as fin:
        for i, line in enumerate(fin):
            name = line.split(':')[0]  # the name of the cluster is the bit before the colon
            if name not in clusters:
                clusters[name] = set()
            # the elements of the cluster are after the colon, space delimited
            nodes = line.split(':')[1].split(' ')[:-1]
            for node in nodes:
                word = str(mapping.loc[int(node)]['word'])
                clusters[name].add(word)

    clusters_list = map(lambda x: ' '.join(list(x)), clusters.values())
    clusters_df = pd.DataFrame(pd.Series(clusters_list, dtype=str), columns=['Set'])

    return clusters_df


lazy_read_output = delayed(read_COS_output_file)


def find_clusters_by_threshold(weighted_edge_list, thresholds=[1]):
    """
    The information we have from the network contains not just structure,
    but weights. These weights tell us how frequently individuals recognize
    the connections which give our network structure. As we vary the threshold
    at which edges are included, we cast a different net for structure.

    Just as n-cliques are nested within (n-1)-cliques, threshold n clusters
    are nested within threshold (n-1) clusters.

    Parameters
    ----------
    weighted_edge_list: dask DataFrame
        with columns:
        - `W1`, `W2`: The words that an edge is between
        - `Count`: the weight of the edge

    min_threshold: int
        will only compute clusters for thresholds above this value.

    Returns
    -------
    clusters: dask DataFrame

    """
    clusters_collector = []
    for threshold in thresholds:
        unweighted = weighted_edge_list[weighted_edge_list['Count'] >= threshold]
        clusters = find_clusters(unweighted[['W1', 'W2']])
        clusters = clusters.assign(threshold=threshold)
        clusters_collector.append(clusters)

    clusters = pd.concat(clusters_collector, ignore_index=True)

    return clusters


def build_cluster_db(weighted_edge_list,
                     output_globstring,
                     thresholds=[1]):
    """
    need to separate by date, then get the clusters dataframe, then add
    the date parameter, then create a unique id by hashing the data, then
    store in hdf


    Parameters
    ----------
    weighted_edge_list: pandas or dask dataframe
    output_globstring:
    thresholds

    Returns
    -------
    clusters: dataframe
    """

    collector = []
    for date in np.array(weighted_edge_list.Date.unique()):
        selection = weighted_edge_list[weighted_edge_list.Date == date]
        clusters = find_clusters_by_threshold(selection,
                                              thresholds=thresholds)
        clusters = clusters.reset_index(drop=True)
        clusters.index = map(lambda x: int(str(date) + str(x)), clusters.index)
        clusters.to_csv(output_globstring.replace('*', str(date)))
        collector.append(clusters)

    return collector


def build_transition_cluster_db(weighted_edge_list,
                                output_globstring,
                                thresholds=[1]):
    """ Identifies clusters formed from edgelists representing
    two consecutive days, returning the clusters that are formed
    from the intersection of those days conversations.

    This is to facilitate identifying how clusters from one day continue
    into the next day.
    """

    collector = []
    dates = np.array(weighted_edge_list.Date.unique())

    for d1, d2 in zip(dates[:-1], dates[1:]):
        selection = weighted_edge_list[int(d1) <= weighted_edge_list.Date <= int(d2)]

        clusters = find_clusters_by_threshold(selection,
                                              thresholds=thresholds)
        clusters = clusters.reset_index(drop=True)
        clusters.index = map(lambda x: int(str(d1) + str(d2) + str(x)), clusters.index)
        clusters.to_csv(output_globstring.replace('*', str(d1) + '_' + str(d2)))
        collector.append(clusters)

    return collector
