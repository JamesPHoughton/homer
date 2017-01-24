



# class ClusterDrawing(object):
#     text_properties = {'size': 12,
#                        'fontname': 'sans-serif',
#                        'horizontalalignment': 'center'}
#
#     def __init__(self, contains, uid=None):
#         if isinstance(contains, (set, frozenset)):  # convenience conversion of set to list.
#             contains = list(contains)
#
#         if isinstance(contains, list):
#             self.is_leaf = False
#             self.contents = []
#             for element in contains:
#                 if isinstance(element, basestring):
#                     self.contents.append(ClusterDrawing(element))
#                 else:
#                     self.contents.append(element)
#             self.linewidth = 1
#         elif isinstance(contains, basestring):
#             self.is_leaf = True
#             # self.text = contains.encode('ascii', 'ignore')
#             self.text = contains
#             self.text = contains.decode('utf-8', 'ignore')
#             self.linewidth = 0
#
#         self.bottom = 0
#         self.center = 0
#         self.pts_buffer = 4
#         self.uid = uid
#
#     def get_list(self):
#         if self.is_leaf:
#             return [self.text]
#         else:
#             return [item for x in self.contents for item in x.get_list()]  # flat list
#
#     def get_set(self):
#         return set(self.get_list())
#
#     def get_by_name(self, name):
#         if self.is_leaf: return None
#
#         if self.uid == name:
#             return self
#         else:
#             for x in self.contents:
#                 obj = x.get_by_name(name)
#                 if obj == None:
#                     continue
#                 else:
#                     return obj
#         return None
#
#     def get_uids(self):
#         if self.is_leaf:
#             return []
#         else:
#             uid_list = [item for x in self.contents for item in x.get_uids()]  # flat list
#             if self.uid != None:
#                 uid_list.append(self.uid)
#             return uid_list
#
#     def score(self):
#         """Get the score for the full (recursive) contents"""
#         score = 0
#         this_list = self.get_list()
#         for word in set(this_list):
#             indices = [i for i, x in enumerate(this_list) if x == word]
#             if len(indices) > 1:
#                 score += sum([abs(a - b) for a, b in itertools.combinations(indices, 2)])
#         return score
#
#     def order(self, scorefunc):
#         """Put the contents in an order that minimizes the score of the whole list"""
#         if not self.is_leaf:
#             best_score = 10000000
#             best_order = self.contents
#             for permutation in itertools.permutations(self.contents):
#                 self.contents = permutation
#                 new_score = scorefunc()
#                 if new_score < best_score:
#                     best_score = new_score
#                     best_order = permutation
#             self.contents = best_order
#
#             [element.order(scorefunc) for element in self.contents]
#
#     def set_height(self, ax):
#         if self.is_leaf:
#             # have to mockup the actual image to get the width
#             self.image_text = ax.text(0, 0, self.text, **self.text_properties)
#             plt.draw()
#             extent = self.image_text.get_window_extent()
#             self.height = extent.y1 - extent.y0
#         else:
#             self.height = (sum([x.set_height(ax) for x in self.contents]) +
#                            (len(self.contents) + 1) * self.pts_buffer)
#         return self.height
#
#     def set_width(self, ax):
#         if self.is_leaf:
#             # have to mockup the actual image to get the width
#             self.image_text = ax.text(0, 0, self.text,
#                                       transform=None, **self.text_properties)
#             plt.draw()
#             extent = self.image_text.get_window_extent()
#             self.width = extent.x1 - extent.x0 + self.pts_buffer
#         else:
#             self.width = max([x.set_width(ax) for x in self.contents]) + 2 * self.pts_buffer
#         return self.width
#
#     def set_center(self, x):
#         if not self.is_leaf:
#             [child.set_center(x) for child in self.contents]
#         self.center = x
#
#     def set_bottom(self, bottom=0):
#         """Sets the bottom of the box.
#         recursively sets the bottoms of the contents appropriately"""
#         self.bottom = bottom + self.pts_buffer
#
#         if not self.is_leaf:
#             cum_height = self.bottom
#             for element in self.contents:
#                 element.set_bottom(cum_height)
#                 cum_height += element.height + self.pts_buffer
#
#     def layout(self, ax):
#         if not self.is_leaf:
#             [child.layout(ax) for child in self.contents]
#
#         plt.box('off')
#         self.set_width(ax)
#         self.set_height(ax)
#         ax.clear()
#
#     def draw(self, ax):
#         if not hasattr(self, 'width'):
#             print 'Must run `layout` method before drawing, preferably with dummy axis'
#
#         if self.is_leaf:
#             self.image_text = ax.text(self.center, self.bottom, self.text,
#                                       transform=None, **self.text_properties)
#         else:
#             [child.draw(ax) for child in self.contents]
#             ax.add_patch(plt.Rectangle((self.center - .5 * self.width, self.bottom),
#                                        self.width, self.height,
#                                        alpha=.1, transform=None))
#         ax.set_axis_off()
#
#
# def make_elements(clustersdf, k_min=0, k_max=25, order=False):
#     prev_elements = []
#     for k, k_group in clustersdf.groupby('k-clique', sort=False):
#         if k < k_min: continue
#         if k > k_max: continue
#         elements = []
#         for i, row in k_group.iterrows():
#             cluster_elements = row['elements']
#             cluster_list = []  # this is what we will eventually pass to the class initialization
#             for prev_element in prev_elements:
#                 prev_set = prev_element.get_set()
#                 if prev_set <= cluster_elements:  # set 'contains'
#                     cluster_elements = cluster_elements - prev_set
#                     cluster_list = cluster_list + [prev_element]
#
#             cluster_list = cluster_list + list(cluster_elements)
#             # print cluster_list
#             elements.append(cluster_drawing(cluster_list, row['id']))
#
#         prev_elements = elements
#
#     a = cluster_drawing(elements)
#     if order:
#         a.order(a.score)
#     return a
#
#
# def draw_transition(a, b, tr_matrix, ax):
#     for a_id in a.get_uids():
#         for b_id in b.get_uids():
#             try:
#                 likelihood = tr_matrix.loc[a_id, b_id]
#             except KeyError:  # if either don't show up in the transition matrix, they don't have a corresponding cluster
#                 continue
#             if likelihood > 0:
#                 # print a_id, b_id, likelihood
#                 a_object = a.get_by_name(a_id)
#                 b_object = b.get_by_name(b_id)
#
#                 ax.plot(
#                     [a_object.center + .5 * a_object.width, b_object.center - .5 * b_object.width],
#                     [a_object.bottom, b_object.bottom],
#                     color='b', alpha=likelihood ** 2, transform=None)
#
#                 ax.plot(
#                     [a_object.center + .5 * a_object.width, b_object.center - .5 * b_object.width],
#                     [a_object.bottom + a_object.height, b_object.bottom + b_object.height],
#                     color='b', alpha=likelihood ** 2, transform=None)
#
#     ax.set_axis_off()
#
