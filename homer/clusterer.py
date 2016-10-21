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

TOOLS_DIR = os.path.dirname(os.path.realpath(__file__))


class TemporaryDirectory(object):
    def __enter__(self):
        self.name = tempfile.mkdtemp()
        return self.name

    def __exit__(self, exc_type, exc_value, traceback):
        shutil.rmtree(self.name)


def find_clusters(unweighted_edge_list):
    """
    Uses COSparallel to identify new clusters from an unweighted edgelist
    Parameters
    ----------
    unweighted_edge_list: dask (or pandas) dataframe with columns 'W1' and 'W2'

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
                '%s/./maximal_cliques %s' % (TOOLS_DIR.replace(' ', '\ '),  # there must be better ways to escape spaces
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
        community_files = glob.glob(dir_name+'/[0-9]*_communities.txt')

        if len(community_files) > 0:
            cluster_list = list()

            for infile in community_files:
                k = int(os.path.basename(infile).rstrip('_communities.txt'))
                clusters = read_COS_output_file(infile, mapping)
                df = pd.DataFrame(pd.Series(clusters), columns=['Set'])
                df['k'] = k
                cluster_list.append(df)

            clusters = pd.concat(cluster_list)
        else:
            clusters = pd.DataFrame(columns=['Set', 'k'])

    return clusters


def read_COS_output_file(infile_name, mapping):
    """
    Parameters
    ----------
    infile_name: basestring
        full path to the

    Reads an output of the COS program (there are multiple)
    and uses the
    take a file output from COS and return a dictionary
    with keys being the integer cluster name, and
    elements being a set of the keywords in that cluster"""

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
                word = mapping.loc[int(node)]['word']
                clusters[name].add(word)
    return clusters


def traverse_thresholds(weighted_edge_list):
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

    Returns
    -------

    """
    thresholds = weighted_edge_list['Count'].unique()
    clusters_collector = []
    for i, t in thresholds.iteritems():
        cluster = delayed(find_clusters_over_threshold)(weighted_edge_list, t)
        clusters_collector.append(cluster)

    clusters = dd.from_delayed(clusters_collector)
    return clusters


def find_clusters_over_threshold(weighted_edge_list, threshold):
    uw_el = weighted_edge_list[weighted_edge_list['Count'] >= threshold]
    clusters = find_clusters(uw_el)
    clusters['threshold'] = threshold
    return clusters