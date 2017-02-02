import numpy as np
import pandas as pd


def build_relations_db(clusters, output_globstring):
    """
    Identifies children (ie, completely contained clusters)




    Parameters
    ----------
    clusters

    Returns
    -------

    """

    # find k-children
    day_th_groups = clusters.groupby(['Date', 'threshold'])
    children = day_th_groups.apply(find_k_children, meta=pd.DataFrame(columns=['children']))

    relations = children

    relations.to_hdf(output_globstring, '/relations', dropna=True)
    return relations


def find_k_children(clusters):
    """
    Identifies which clusters are direct children of a parent cluster

    Children have a 'k' value which is one higher than the parent cluster,
    and are completely contained within the parent cluster
    and share the same threshold?

    This corresponds to components of the conversation with closer semantic
    connectivity, but the same level of popularity.

    Parameters
    ----------
    clusters: dataframe

    Returns
    -------
    children_df: pandas dataframe
        Index is the ID of the parent
        'children' column contains a list of the ids of next generation children.

    """
    collector = []
    for k in sorted(np.array(clusters.k.unique()))[:-1]:
        parents = clusters[clusters.k == k]
        child_candidates = clusters[clusters.k == k + 1]
        for parent_id, parent in parents.iterrows():
            parent_set = set(parent['Set'].split(' '))
            children = [child_id for child_id, child in child_candidates.iterrows() if
                        set(child['Set'].split(' ')) < parent_set]
            collector.append({'ID': parent_id,
                              'children': children})

    children_df = pd.DataFrame(collector, columns=['ID', 'children'])
    # include col names to account for empty case
    children_df.set_index('ID', inplace=True)
    return children_df


def find_k_parents(children):
    """
    Reshapes the children column to identify parents of a particular
    cluster.

    Parents have a 'k' value which is one lower than the child cluster
    and completely contain the child cluster


    Parameters
    ----------
    children

    Returns
    -------

    """
    raise NotImplementedError


def find_t_children(clusters):
    """
    finds clusters which are completely contained within the parent cluster
     and have a threshold value one higher than


    this corresponds to the components of the conversation which are more
    popular, but with the same level of semantic connectivity.


    Parameters
    ----------
    clusters

    Returns
    -------

    """


def find_t_parents(t_children):
    raise NotImplementedError


def find_overlapping(clusters):
    raise NotImplementedError