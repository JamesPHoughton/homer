import json
import gzip
import matplotlib.pylab as plt
import numpy as np


class Cluster(object):
    def __init__(self,
                 contents,
                 k=None,
                 w=None,
                 date=None,
                 is_leaf=False):
        self.contents = contents
        self.k = k  # k-clique clustering parameter
        self.w = w  # threshold
        self.date = date
        self.is_leaf = is_leaf

        self.k_children = []
        self.w_children = []
        self.k_parents = []
        self.t_parents = []
        self.tomorrow = []
        self.p_tomorrow = []
        self.yesterday = []
        self.p_yesterday = []
        self.members = []

        self.left = None
        self.right = None

        self.pts_buffer = 4
        self.height = None
        self.child_bottoms = None
        self.width = None
        self.image_text = None

    def __repr__(self):
        return str(self.contents)

    def find(self, keyword):
        """
        query: string to find a particular keyword
        """
        if keyword > self.contents:
            try:
                return self.right.find(keyword)
            except:
                return None
        elif keyword < self.contents:
            try:
                return self.left.find(keyword)
            except:
                return None
        elif keyword == self.contents:
            return self

    def insert(self, cluster_obj):
        """ for inserting leaves """
        if cluster_obj.contents > self.contents:
            try:
                self.right.insert(cluster_obj)
            except:  # right is none, so set it to the new object
                self.right = cluster_obj
        elif cluster_obj.contents < self.contents:
            try:
                self.left.insert(cluster_obj)
            except:  # left is none, so set it to the new object
                self.left = cluster_obj
        elif cluster_obj.contents == self.contents:
            raise ValueError('Already have this one. Should probably implement merge here.')

    def get_k_members(self):
        if self.is_leaf:
            yield self
        else:
            for n in self.k_children:
                for m in n.get_k_members():
                    yield m

    def to_json(self):
        """Uses abbreviated field names to shrink file sizes"""
        return json.dumps({
            'cn': str(self.contents),
            'k': self.k,
            'w': self.w,
            'dt': self.date,
            'kch': [str(ch) for ch in self.k_children],
            'lf': self.is_leaf
        })

    # ###### Drawing Functions ######
    text_properties = {'size': 12,
                       'fontname': 'sans-serif',
                       'horizontalalignment': 'center'}

    def compute_size(self):
        if self.height is None or self.width is None:
            if self.is_leaf:
                f = plt.figure()
                r = f.canvas.get_renderer()
                t = plt.text(0.5, 0.5, self.contents, **self.text_properties)

                bb = t.get_window_extent(renderer=r)
                self.width = bb.width
                self.height = bb.height
                plt.close()

            else:
                heights, widths = zip(*[x.compute_size() for x in self.k_children])
                self.height = sum(heights) + self.pts_buffer
                self.width = max(widths) + 2 * self.pts_buffer
                self.child_bottoms = np.cumsum([self.pts_buffer] + list(heights[:-1]))

        return self.height, self.width

    def draw(self, ax, center, bottom):
        if self.height is None or self.width is None or self.child_bottoms is None:
            self.compute_size()

        if self.is_leaf:
            self.image_text = ax.text(center, bottom, self.contents,
                                      transform=None, **self.text_properties)
        else:
            [child.draw(ax, center, bottom + child_bottom)
             for child, child_bottom in zip(self.k_children, self.child_bottoms)]
            ax.add_patch(plt.Rectangle((center - .5 * self.width, bottom),
                                       self.width, self.height-self.pts_buffer/2,
                                       alpha=.1, transform=None))
        ax.set_axis_off()


def apply_to_tree(node, f, how='center'):
    if node is None:
        return
    if how is 'left':
        f(node)
    apply_to_tree(node.left, f)
    if how is 'center':
        f(node)
    apply_to_tree(node.right, f)
    if how is 'right':
        f(node)


def walk_tree(tree, how='infix'):
    if tree is None:
        return None

    if how is 'prefix':
        yield tree

    for elem in walk_tree(tree.left, how):
        yield elem

    if how is 'infix':
        yield tree

    for elem in walk_tree(tree.right, how):
        yield elem

    if how is 'postfix':
        yield tree


def walk_k_ancestry(tree, order='bottom up'):
    if order == 'top down':
        yield tree

    for child in tree.k_children:
        for elem in walk_k_ancestry(child, order):
            yield elem

    if order == 'bottom up':
        yield tree

    if order not in ['top down', 'bottom up']:
        raise ValueError('Bad Value for "order"')


def compute_tree(clusters, relations, tree_filename):
    # put clusters in a tree
    root = Cluster('__root__')
    for ID, row in clusters.iterrows():
        new = Cluster(contents=str(ID),
                      k=row['k'],
                      w=row['threshold'],
                      date=row['Date'])

        root.insert(new)
        if row['k'] == 3:
            root.k_children.append(new)

    # add clusters as children
    for (_, _, ID), row in relations.iterrows():
        node = root.find(str(ID))
        for child in row['children']:
            node.k_children.append(root.find(str(child)))

    # add leaves (words)
    for node in walk_k_ancestry(root, 'bottom up'):
        if node.contents != '__root__':
            present_in_children = [m.contents for m in node.get_k_members()]
            words = clusters['Set'].loc[int(node.contents)].compute().values[0].split(
                ' ')
            for leaf_word in list(set(words) - set(present_in_children)):
                leaf = root.find(leaf_word)
                if leaf is None:
                    leaf = Cluster(leaf_word, is_leaf=True)
                    root.insert(leaf)
                node.k_children.append(leaf)

    with gzip.open(tree_filename, 'wb') as f:
        for node in walk_tree(root):
            f.write(node.to_json().encode())

    return root


def load_tree(tree_filename):
    with gzip.open(tree_filename, 'rb') as f:
        line = f.readline()
        info = json.loads(line.decode())
        root = Cluster(contents=info['cn'], k=info['k'],
                       w=info['w'], date=info['dt'], is_leaf=info['lf'])
        root.k_children = info['kch']
        for line in f:
            info = json.loads(line.decode())
            node = Cluster(contents=info['cn'], k=info['k'],
                           w=info['w'], date=info['dt'], is_leaf=info['lf'])
            node.k_children = info['kch']
            root.insert(node)

    for node in walk_tree(root):
        node.k_children = [root.find(x) for x in node.k_children]

    return root
