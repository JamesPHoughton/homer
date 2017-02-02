import pandas as pd





def compute_transition_list(daily_clusters, inter_day_clusters):
    collector = []

    for ID, cluster in inter_day_clusters.iterrows():
        d1_candidates = daily_clusters[
            daily_clusters.k == cluster.k and
            daily_clusters.threshold <= cluster.threshold and
            daily_clusters.Date == cluster.Date_1
            ]

        d2_candidates = daily_clusters[
            daily_clusters.k == cluster.k and
            daily_clusters.threshold <= cluster.threshold and
            daily_clusters.Date == cluster.Date_2
            ]

        inter_day_set = set(cluster['Set'].split(' '))

        d1_children = [child for child in d1_candidates.iterrows() if
                       set(child[1]['Set'].split(' ')) < inter_day_set]

        d2_children = [child for child in d2_candidates.iterrows() if
                       set(child[1]['Set'].split(' ')) < inter_day_set]

        for d1_ID, d1_child in d1_children:
            d1_set = set(d1_child['Set'].split(' '))
            for d2_ID, d2_child in d2_children:
                if (d2_child['k'] == d1_child['k'] and
                        d2_child['threshold'] == d1_child['threshold']):

                    d2_set = set(d2_child['Set'].split(' '))
                    intersection = len(d1_set.intersection(d2_set))
                    if intersection > 0:
                        union = len(d1_set.union(d2_set))
                        jaccard = intersection / union
                        collector.append({
                            'C1': d1_ID,
                            'C2': d2_ID,
                            'similarity': jaccard
                        })
    transition_list = pd.DataFrame(collector).drop_duplicates()

    return transition_list
