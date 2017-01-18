homer
=====
This package is useful for identifying structures of meaning and conversations from textual data. It works by constructing a network of concepts, and parsing the network down into sets of clusters, or conversations, that interact with one another.


## End user facing components
### Identifying clusters to work with
 - Find clusters/conversations that contain a word or words
 ```python
 homer.get_clusters_by_keyword(all_words={'bob', 'stuart', 'kevin'}, 
                               any_words=None,
                               first_date=20150101,
                               last_date=20150102)
 ```

 - Get clusters by id
 ```python
 homer.get_clusters_by_id(cluster_ids=['1a97e4'])
 ```
 
 - For a given cluster, or set of clusters, find all the children of the cluster 
   ie, say, what are the major areas of disagreement or of discussion within 
   this conversational cluster?
```python
homer.get_child_clusters(cluster_ids=[1a97e4], generations=1)
```
   
 - find the parents of the cluster - to say, what larger conversations is
   this cluster a part of?
```python
homer.get_parent_clusters(cluster_ids=[1a97e4], generations=1)
```

- get sibling clusters - find the clusters that are the same level as the
focal cluster, and share an ancestor `generations` ago.
```python
homer.get_sibling_clusters(cluster_ids=[1a97e4], generations=1)
```

- get overlapping clusters - find the subset of siblings which overlap by `n` nodes
```python
homer.get_overlapping_clusters(cluster_ids=[1a97e4], n=1)
```

- get subsequent/prior clusters with transition likelihoods
```python
homer.get_next(cluster_ids=[1a97e4])
homer.get_previous(cluster_ids=[1a97e4])
```



### Working with the underlying network
- Get the underlying network
 ```python
 homer.get_network(cluster_ids=[1a97e4, 1a97e5, 1a97e6])
 ```

### Working with volume trends
- Get volume for a list of clusters (ie, the sum of the weights in the cluster edges)
```python
homer.get_edge_weights(cluster_ids=[1a97e4, 1a97e5, 1a97e6])
```

### Visualization
 - Plot the conversational structure as nested sets of words.
 ```python
 homer.draw.nested_sets(cluster_ids=[1a97e4, 1a97e5, 1a97e6], 
                        include_children=False)
 ```
 If the clusters are on separate days, they will be plotted on separate
 columns. To add transition markers:
 ```python
homer.draw.nested_sets(cluster_ids=[1a97e4, 1a97e5, 1a97e6, ...], 
                        include_children=False,
                        transition_matrices=[df_1_2, df_2_3])
```

 - Plot the conversational structure as a network diagram. This can only be
 for a single day.
 ```python
 homer.draw.network(cluster_ids=[1a97e4, 1a97e5, 1a97e6])
 ```


 
## Prep work

- Data Collection
```python
homer.collect_twitter()
```

- Get word cooccurrences from twitter json messages
```python
homer.get_message_cooccurrences(message_json, hashtags_only=True)
```

- Identifying weighted edgelists
```python
homer.get_weighted_edgelist(tw_files,
                            hashtags_only=False)

```

- build a weighted edgelists database:
```python
homer.build_weighted_edgelist_db(tw_file_globstring, 
                                 output_globstring,
                                 hashtags_only=False)
```

- Identifying clusters day by day
- Creating transition matrices


## Down the road:
- build clusters on different timescales than a single day
- Add parser for different types of data