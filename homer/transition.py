import pandas as pd
import dask



def handle_transition_cluster(daily_clusters, cluster):
    collector = []

    d1_candidates = daily_clusters[
        (daily_clusters.k == cluster.k) &
        (daily_clusters.threshold <= cluster.threshold) &
        (daily_clusters.Date == cluster.Date_1)
        ]

    d2_candidates = daily_clusters[
        (daily_clusters.k == cluster.k) &
        (daily_clusters.threshold <= cluster.threshold) &
        (daily_clusters.Date == cluster.Date_2)
        ]

    inter_day_set = set(cluster['set'].split(' '))

    d1_children = [child for child in d1_candidates.iterrows() if
                   set(child[1]['set'].split(' ')) < inter_day_set]

    d2_children = [child for child in d2_candidates.iterrows() if
                   set(child[1]['set'].split(' ')) < inter_day_set]

    for _, d1_child in d1_children:
        d1_set = set(d1_child['set'].split(' '))
        for _, d2_child in d2_children:
            if (d2_child['k'] == d1_child['k'] and
                        d2_child['threshold'] == d1_child['threshold']):

                d2_set = set(d2_child['set'].split(' '))
                intersection = len(d1_set.intersection(d2_set))
                if intersection > 0:
                    union = len(d1_set.union(d2_set))
                    jaccard = intersection / union
                    collector.append({
                        'C1': d1_child['ID'],
                        'C2': d2_child['ID'],
                        'similarity': jaccard
                    })

    if len(collector) > 0:
        return pd.DataFrame(collector).drop_duplicates()
    else:
        return pd.DataFrame(columns=['C1', 'C2', 'similarity'])

lazy_handle_transition_cluster = dask.delayed(handle_transition_cluster)


def compute_transition_list(daily_clusters, inter_day_clusters, output_filename):
    dask_collector = []
    for _, cluster in inter_day_clusters.iterrows():  # parallelize here
        dask_collector.append(lazy_handle_transition_cluster(daily_clusters,
                                                             cluster))

    template = pd.DataFrame([{'C1': 'str', 'C2': 'str', 'similarity': 0.3}],
                            columns=['C1', 'C2', 'similarity'])

    with dask.diagnostics.ProgressBar():
        ddf = dask.dataframe.from_delayed(dask_collector, meta=template)
        df = ddf.compute(get=dask.multiprocessing.get)
        df.drop_duplicates().to_csv(output_filename, index=False)

    return df
