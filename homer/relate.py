import pandas as pd
import dask


def find_k_children(parent_file, child_file):
    parents = pd.read_csv(parent_file, header=None, names=['ID', 'set'], index_col=['ID'])
    parents['set'] = parents['set'].apply(lambda x: set(x.split(' ')))
    child_candidates = pd.read_csv(child_file, header=None, names=['ID', 'set'], index_col=['ID'])
    child_candidates['set'] = child_candidates['set'].apply(lambda x: set(x.split(' ')))

    collector = []
    for parent_id, parent in parents.iterrows():
        children = [child_id for child_id, child in child_candidates.iterrows() if
                    child['set'].issubset(parent['set'])]
        if len(children) > 0:
            collector.append({'ID': parent_id,
                              'children': children})

    #return pd.DataFrame(collector)
    return collector


lazy_find_k_children = dask.delayed(find_k_children)


def build_day(cluster_files_df, output_file):
    current_group = cluster_files_df.sort(columns='k').reset_index(drop=True)
    dask_collector = []
    for ia, ib in zip(current_group.index[:-1], current_group.index[1:]):
        dask_collector.append(lazy_find_k_children(cluster_files_df['file'].loc[ia],
                                                   cluster_files_df['file'].loc[ib]))

    template = pd.DataFrame([{'ID': 'nice str', 'children': 'difficult str'}],
                            columns=['ID', 'children'])

    dask.bag.from_delayed(dask_collector).to_file(output_file)


def build_children_db(cluster_files_df, output_globstring):
    """
    Identifies children (ie, completely contained clusters)




    Parameters
    ----------
    clusters

    Returns
    -------

    """

    children_filenames_collector = []

    for (date, threshold), group in cluster_files_df.groupby(by=['date', 'threshold']):
        children_filename = output_globstring.replace('*', '_%s_%s' % (date, threshold))
        build_day(group, children_filename)
        children_filenames_collector.append(children_filename)

    return children_filenames_collector

