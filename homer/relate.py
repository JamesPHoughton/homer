def build_relatives_db(clusters):
    """
    Identifies children (ie, completely contained clusters)




    Parameters
    ----------
    clusters

    Returns
    -------

    """

    # Split by timeperiod



def find_k_children(clusters):
    """
    Identifies which clusters are direct children of a parent cluster

    Children have a 'k' value which is one higher than the parent cluster,
    and are completely contained within the parent cluster
    and share the same threshold?

    Adds a column to the dataframe which is a list of cluster ids

    This corresponds to components of the conversation with closer semantic
    connectivity, but the same level of popularity.

    Parameters
    ----------
    clusters

    Returns
    -------

    """
    # split by threshold (do independent analyses, perhaps in parallel)

    # split by k, look for children in higher k
    raise NotImplementedError

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

def find_overlapping(clusters)