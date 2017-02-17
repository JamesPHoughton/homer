import json
import gzip
import matplotlib.pylab as plt
import numpy as np
import matplotlib.path as mpath
import matplotlib.patches as mpatches
import matplotlib as mpl


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
        # self.w_children = []
        # self.k_parents = []
        # self.t_parents = []
        self.tomorrow = []
        self.p_tomorrow = []
        # self.yesterday = []
        # self.p_yesterday = []
        self.members = []

        self.left = None
        self.right = None

        self.pts_buffer = 4
        self.height = None
        self.child_bottoms = None
        self.width = None

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
            'lf': self.is_leaf,
            'tm': [str(t) for t in self.tomorrow],
            'ptm': [float(p) for p in self.p_tomorrow]
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
            ax.text(center, bottom, self.contents,
                    transform=None, **self.text_properties)
        else:
            [child.draw(ax, center, bottom + child_bottom)
             for child, child_bottom in zip(self.k_children, self.child_bottoms)]
            ax.add_patch(plt.Rectangle((center - .5 * self.width, bottom),
                                       self.width, self.height - self.pts_buffer / 2,
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


def compute_tree(clusters, relations, transitions, tree_filename):
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
        node = root.find('_'+str(ID))
        for child in row['children']:
            node.k_children.append(root.find(str(child)))

    # add leaves (words)
    for node in walk_k_ancestry(root, 'bottom up'):
        if node.contents != '__root__':
            present_in_children = [m.contents for m in node.get_k_members()]
            words = clusters['Set'].loc[int(node.contents[1:])].compute().values[0].split(
                ' ')
            for leaf_word in list(set(words) - set(present_in_children)):
                leaf = root.find(leaf_word)
                if leaf is None:
                    leaf = Cluster(leaf_word, is_leaf=True)
                    root.insert(leaf)
                node.k_children.append(leaf)

    # add connections to subsequent day clusters
    for i, (c1, c2, similarity) in transitions.iterrows():
        n1 = root.find('_'+str(int(c1)))
        n2 = root.find('_'+str(int(c2)))
        n1.tomorrow.append(n2)
        n1.p_tomorrow.append(similarity)

    with gzip.open(tree_filename, 'wb') as f:
        for node in walk_tree(root):
            f.write((node.to_json() + '\n').encode())

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
            node.tomorrow = info['tm']
            node.p_tomorrow = info['ptm']
            root.insert(node)

    for node in walk_tree(root):
        node.k_children = [root.find(x) for x in node.k_children]
        node.tomorrow = [root.find(x) for x in node.tomorrow]

    return root


def connect(ax,
            node_t1, center_t1, bottom_t1,
            node_t2, center_t2, bottom_t2,
            p=1,
            base_alpha=.25):
    left = center_t1 + .5 * node_t1.width
    right = center_t2 - .5 * node_t2.width
    top_t1 = bottom_t1 + node_t1.height
    top_t2 = bottom_t2 + node_t2.height
    center = (left + right) / 2

    Path = mpath.Path
    path_data = [
        (Path.MOVETO, (left, bottom_t1 + node_t1.pts_buffer / 2)),
        (Path.CURVE4, (center, bottom_t1 + node_t1.pts_buffer / 2)),
        (Path.CURVE4, (center, bottom_t2 + node_t1.pts_buffer / 2)),
        (Path.CURVE4, (right, bottom_t2 + node_t1.pts_buffer / 2)),
        (Path.LINETO, (right, top_t2 - node_t1.pts_buffer)),
        (Path.CURVE4, (center, top_t2 - node_t1.pts_buffer)),
        (Path.CURVE4, (center, top_t1 - node_t1.pts_buffer)),
        (Path.CURVE4, (left, top_t1 - node_t1.pts_buffer)),
        (Path.CLOSEPOLY, (left, bottom_t1 + node_t1.pts_buffer / 2))
    ]
    codes, verts = zip(*path_data)
    path = mpath.Path(verts, codes)
    patch = mpatches.PathPatch(path, facecolor='grey',
                               linewidth=.5, alpha=base_alpha * p,
                               transform=None)
    ax.add_patch(patch)

    for i_t1, child_t1 in enumerate(node_t1.k_children):
        for i_t2, child_t2 in enumerate(node_t2.k_children):
            for i_t1_t2, t1_t2 in enumerate(child_t1.tomorrow):
                if t1_t2 == child_t2:
                    connect(ax,
                            child_t1,
                            center_t1,
                            bottom_t1 + node_t1.child_bottoms[i_t1],
                            child_t2,
                            center_t2,
                            bottom_t2 + node_t2.child_bottoms[i_t2],
                            p=child_t1.p_tomorrow[i_t1_t2],
                            base_alpha=base_alpha)


def draw_series(cluster, n_days,
                spacing=150):
    dpi = mpl.rcParams['figure.dpi']

    cluster_list = [cluster]
    for d in range(n_days):
        cluster_list.append(cluster_list[-1].tomorrow[np.argmax(cluster_list[-1].p_tomorrow)])

    heights, widths = zip(*[cl.compute_size() for cl in cluster_list])

    fig_height = np.max(heights) / dpi + 1
    fig_width = (0.5 * (widths[0] + widths[-1]) + n_days * spacing) / dpi + 1

    fig = plt.figure(figsize=(fig_width, fig_height))
    ax = fig.add_axes([0., 0., 1., 1.])

    for col in range(len(cluster_list)):
        center = .5 * dpi + .5 * widths[0] + spacing * col
        bottom = .5 * (fig_height * dpi - heights[col])
        cluster_list[col].draw(ax, center, bottom)

        if col < n_days:
            tm_center = .5 * dpi + .5 * widths[0] + spacing * (col + 1)
            tm_bottom = .5 * (fig_height * dpi - heights[col + 1])

            connect(ax,
                    cluster_list[col], center, bottom,
                    cluster_list[col + 1], tm_center, tm_bottom,
                    p=np.max(cluster_list[col].p_tomorrow))
