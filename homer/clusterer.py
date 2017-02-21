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
import dask

TOOLS_DIR = os.path.dirname(os.path.realpath(__file__))


class TemporaryDirectory(object):
    def __enter__(self):
        self.name = tempfile.mkdtemp()
        return self.name

    def __exit__(self, exc_type, exc_value, traceback):
        shutil.rmtree(self.name)


def run_cluster_algorithm(unweighted_edge_list, dir_name):
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
            '%s/./cos -P 16 %s.mcliques' % (TOOLS_DIR.replace(' ', '\ '),
                                            edgelist_filename.replace(' ', '\ '))]

    response = subprocess.check_output(r'; '.join(cmds), shell=True)

    return response


def read_COS_output_file(infile_name, outfile_name, mapping, cluster_id_base):
    """
    Reads an output of the COS program (there are multiple)
    and outputs a csv file which has built clusters from that
    input

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
                clusters[name].add(node)  # set takes care of repeats

    with open(outfile_name, 'w') as fout:
        for key, cluster_set in clusters.items():
            cluster_id = cluster_id_base + '_' + key
            words = [mapping.loc[int(element)]['word'] for element in cluster_set]
            fout.write(cluster_id + ', ' + ' '.join(words) + '\n')


lazy_read_output = delayed(read_COS_output_file)


def process_cluster_algorithm_output(cos_raw_dir, cluster_dir, date, threshold):
    # read the mapping file created by 'maximal_cliques'
    map_filename = glob.glob(cos_raw_dir + '/*.map')

    mapping = pd.read_csv(map_filename[0], sep=' ', header=None,
                          names=['word', 'number'],
                          index_col='number')

    # read the community files
    community_files = glob.glob(cos_raw_dir + '/[0-9]*_communities.txt')

    if len(community_files) > 0:
        dask_collector = []
        outfiles_collector = []

        for infile in community_files:
            k = os.path.basename(infile).rstrip('_communities.txt')
            outfile = cluster_dir + '/' + k + '_named_communities.csv'
            cluster_id_base = '__%s_%s_%s' % (date, k, str(threshold))
            dask_collector.append(lazy_read_output(infile, outfile, mapping, cluster_id_base))
            outfiles_collector.append({'k':k, 'date':date,
                                       'threshold': threshold,
                                       'file': outfile})

        dask.compute(*dask_collector, get=dask.multiprocessing.get)

    return outfiles_collector

def find_clusters_by_threshold(weighted_edge_list,
                               date,
                               thresholds=[1],
                               working_directory=None,
                               output_directory=None
                               ):
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
    cluster_file_collector = []
    for threshold in thresholds:
        unweighted = weighted_edge_list[weighted_edge_list['Count'] >= threshold][['W1','W2']]

        th_working_directory = working_directory + '/' + str(threshold)
        os.makedirs(th_working_directory, exist_ok=True)

        run_cluster_algorithm(unweighted, th_working_directory)

        th_output_directory = output_directory + '/' + str(threshold)
        os.makedirs(th_output_directory, exist_ok=True)

        outfiles = process_cluster_algorithm_output(th_working_directory, th_output_directory,
                                                    date, threshold)
        cluster_file_collector = cluster_file_collector + outfiles
    return cluster_file_collector


def build_cluster_db(weighted_edge_list_files,
                     intermediate_files_directory,
                     output_files_directory,
                     dates,
                     thresholds=[1]):
    """

    Parameters
    ----------
    weighted_edge_list_files: list of filenames
        should be in the same order as 'dates', and these should be
        consecutive ascending
    intermediate_files_directory: root directory where directories for
        intermediate file dates will be created
        no trailing slash
        e.g. '../working/test'
    output_files_globstring:
        where output files should be located
        csv extension
        e.g. '../working/test/test_clusters_*.csv'
    dates: list of integers 20150615 style
    thresholds: list of integers
        one entry for each threshold that you want clusters calculated for

    """

    collector = []
    for w_el_file, date in zip(weighted_edge_list_files, dates):
        w_el = pd.read_csv(w_el_file)

        working_directory = intermediate_files_directory + '/' + str(date)
        os.makedirs(working_directory, exist_ok=True)

        date_output_directory = output_files_directory + '/' + str(date)
        os.makedirs(date_output_directory, exist_ok=True)

        cluster_files = find_clusters_by_threshold(
            weighted_edge_list=w_el,
            thresholds=thresholds,
            working_directory=working_directory,
            output_directory=date_output_directory,
            date=date)

        collector = collector + cluster_files

    return collector


def build_transition_cluster_db(weighted_edge_list_files,
                                intermediate_files_directory,
                                output_files_directory,
                                dates,
                                thresholds=[1],
                                ):
    """ Identifies clusters formed from edgelists representing
    two consecutive days, returning the clusters that are formed
    from the intersection of those days conversations.

    This is to facilitate identifying how clusters from one day continue
    into the next day.
    """

    collector = []

    for d1, w_el_1, d2, w_el_2 in zip(dates[:-1], weighted_edge_list_files[:-1],
                                      dates[1:], weighted_edge_list_files[1:]):
        w_el = pd.concat([pd.read_csv(w_el_1), pd.read_csv(w_el_2)])

        working_directory = intermediate_files_directory + '/' + str(d1)+'_'+str(d2)
        os.makedirs(working_directory, exist_ok=True)

        date_output_directory = output_files_directory + '/' + str(d1)+'_'+str(d2)
        os.makedirs(date_output_directory, exist_ok=True)

        cluster_files = find_clusters_by_threshold(
            weighted_edge_list=w_el,
            thresholds=thresholds,
            working_directory=working_directory,
            output_directory=date_output_directory,
            date=str(d1)+'_'+str(d2)
        )

        collector = collector + cluster_files

    return collector
