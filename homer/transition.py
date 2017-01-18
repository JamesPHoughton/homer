import clusterer

def calculate_transition_matrix(joint_unweighted_edgelist,
                                cluster_set_1,
                                cluster_set_2):
    """
    Clusters in time period 1 and clusters in time period 2 should each
    be subsets of clusters on the joint network from both time periods.


    Parameters
    ----------
    joint_unweighted_edgelist: dask or pandas dataframe
        unweighted edgelist for both time periods with columns 'W1' and 'W2'
    cluster_set_1
    cluster_set_2

    Returns
    -------
    dictionary of transition matricies, with keys being the 'k' value

    """
    joint_clusters = clusterer.find_clusters(joint_unweighted_edgelist[['W1', 'W2']])
