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
            df = unweighted_edge_list.repartition(npartitions=1)

            # write the unweighted edgelist to a file
            glob_filename = 'unweighted_edge_list.*.txt'
            edgelist_filename = glob_filename.replace('*', '0')

            df.to_csv(dir_name + '/' + glob_filename, sep=' ',
                      index=False, header=False, encoding='utf-8')

        elif isinstance(unweighted_edge_list, pd.DataFrame):
            df = unweighted_edge_list

            edgelist_filename = 'unweighted_edge_list.txt'
            df.to_csv(dir_name + '/' + edgelist_filename, sep=' ',
                      index=False, header=False, encoding='utf-8')

        # run .mcliques
        # for each output file, run cosparallel
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
            cluster_list = list()

            for infile in community_files:
                k = int(os.path.basename(infile).rstrip('_communities.txt'))
                clusters = read_COS_output_file(infile, mapping)
                clusters_list = map(lambda x: ' '.join(list(x)), clusters.values())
                df = pd.DataFrame(pd.Series(clusters_list, dtype=str), columns=['Set'])
                df['k'] = k
                cluster_list.append(df)

            clusters_df = pd.concat(cluster_list)
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

    # todo: this could be rewritten with dask, to read rows into a df,
    # and then sum those with the same index name.
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
    return clusters


def find_clusters_for_any_threshold(weighted_edge_list, min_threshold=1):
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
    #todo: this might be reasonable to sort in k order?
    thresholds = weighted_edge_list['Count'].unique()
    clusters_collector = []
    for t in np.array(thresholds):
        if t < min_threshold:
            continue
        cluster = delayed(find_clusters_over_threshold)(weighted_edge_list, t)
        clusters_collector.append(cluster)

    clusters = dd.from_delayed(clusters_collector)
    return clusters


def find_clusters_over_threshold(weighted_edge_list, threshold):
    """
    Find clusters where all edges in the cluster have at least 'threshold' occurrances
    in the dataset.

    """

    uw_el = weighted_edge_list[weighted_edge_list['Count'] >= threshold]
    clusters = find_clusters(uw_el[['W1', 'W2']])
    clusters['threshold'] = threshold
    return clusters


def build_cluster_db(weighted_edge_list,
                     output_globstring,
                     min_threshold=1,
                     hashtags_only=False):
    """
    need to separate by date, then get the clusters dataframe, then add
    the date parameter, then create a unique id by hashing the data, then
    store in hdf


    Parameters
    ----------
    weighted_edge_list
    min_threshold
    by

    Returns
    -------
    clusters: dataframe
    """

    collector = []
    for date in np.array(weighted_edge_list.Date.unique()):
        selection = weighted_edge_list[weighted_edge_list.Date == date]
        df = find_clusters_for_any_threshold(selection,
                                             min_threshold=min_threshold)

        if df.ndim > 0:
            df_2 = df.assign(Date=date)
            df_3 = df_2.assign(ID=df_2.apply(lambda x: hash(tuple(x)), axis=1,
                                             columns='hash'))
            df_4 = df_3.set_index('ID')

            collector.append(df_4)

    clusters = dd.concat(collector, interleave_partitions=True)
    clusters = clusters.repartition(npartitions=1)  # Todo: This partitioning is problematic

    clusters.to_hdf(output_globstring, '/clusters', dropna=True)

    return clusters

