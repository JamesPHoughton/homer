import matplotlib.pylab as plt
import itertools


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

        self.bottom = 0
        self.center = 0
        self.pts_buffer = 4
        self.height = None
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


    # ###### Drawing Functions ######
    text_properties = {'size': 12,
                       'fontname': 'sans-serif',
                       'horizontalalignment': 'center'}

    def set_height(self, ax):
        if self.is_leaf:
            # have to mockup the actual image to get the width
            self.image_text = ax.text(0, 0, self.contents, **self.text_properties)
            plt.draw()
            extent = self.image_text.get_window_extent()
            self.height = extent.y1 - extent.y0
        else:
            self.height = (sum([x.set_height(ax) for x in self.k_children]) +
                           (len(self.k_children) + 1) * self.pts_buffer)
        return self.height

    def set_width(self, ax):
        if self.is_leaf:
            # have to mockup the actual image to get the width
            self.image_text = ax.text(0, 0, self.contents,
                                      transform=None, **self.text_properties)
            plt.draw()
            extent = self.image_text.get_window_extent()
            self.width = extent.x1 - extent.x0 + self.pts_buffer
        else:
            self.width = (max([x.set_width(ax) for x in self.k_children]) +
                          2 * self.pts_buffer)
        return self.width

    def set_center(self, x):
        if not self.is_leaf:
            [child.set_center(x) for child in self.k_children]
        self.center = x

    def set_bottom(self, bottom=0):
        """Sets the bottom of the box.
        recursively sets the bottoms of the contents appropriately"""
        self.bottom = bottom + self.pts_buffer

        if not self.is_leaf:
            cum_height = self.bottom
            for element in self.k_children:
                element.set_bottom(cum_height)
                cum_height += element.height + self.pts_buffer

    def layout(self, ax):
        if not self.is_leaf:
            [child.layout(ax) for child in self.k_children]

        plt.box('off')
        self.set_width(ax)
        self.set_height(ax)
        ax.clear()

    def draw(self, ax):
        if not hasattr(self, 'width'):
            raise AttributeError(
                'Must run `layout` method before drawing, preferably with dummy axis')

        if self.is_leaf:
            self.image_text = ax.text(self.center, self.bottom, self.contents,
                                      transform=None, **self.text_properties)
        else:
            [child.draw(ax) for child in self.k_children]
            ax.add_patch(plt.Rectangle((self.center - .5 * self.width, self.bottom),
                                       self.width, self.height,
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
