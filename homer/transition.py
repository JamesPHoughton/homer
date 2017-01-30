import pandas as pd


# def build_transitions_db(daily_clusters, intra_day_clusters):
#     date_pairs = intra_day_clusters[['Date_1', 'Date_2']].drop_duplicates()
#     for index, (d1, d2) in date_pairs.iterrows:
#         tmat = compute_transition_matrix(
#             clusters_d1=daily_clusters[daily_clusters['Date'] == d1],
#             clusters_d2=daily_clusters[daily_clusters['Date'] == d2],
#             clusters_d12=intra_day_clusters[
#                 intra_day_clusters['Date_1'] == d1 and
#                 intra_day_clusters['Date_2'] == d2
#                 ]
#         )
#
#
# def compute_transition_matrix(clusters_d1, clusters_d2, clusters_d12):
#     transition_matrix = pd.DataFrame()
#
#     for ID, cluster in clusters_d12.iterrows():
#         d1_candidates = clusters_d1[
#             clusters_d1.k == cluster.k and
#             clusters_d1.w <= cluster.w
#             ]
#         inter_day_set = set(cluster['Set'].split(' '))
#
#         d1_children = [child for child in d1_candidates.iterrows() if
#                        set(child['Set'].split(' ')) < inter_day_set]
#
#         d2_candidates = clusters_d2[
#             clusters_d2.k == cluster.k and
#             clusters_d2.w <= cluster.w
#             ]
#
#         d2_children = [child for child in d2_candidates.iterrows() if
#                        set(child['Set'].split(' ')) < inter_day_set]
#
#         for d1_ID, d1_child in d1_children:
#             d1_set = set(d1_child['Set'].split(' '))
#             for d2_ID, d2_child in d2_children:
#                 d2_set = set(d2_child['Set'].split(' '))
#                 jaccard = len(d1_set.union(d2_set)) / len(d1_set.intersection(d2_set))
#                 transition_matrix.set_value(index=d1_ID,
#                                             col=d2_ID,
#                                             value=jaccard)
#
#     return transition_matrix


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
    transition_list = pd.DataFrame(collector)

    return transition_list
