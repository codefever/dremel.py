#!/usr/bin/env python

import typing


class Node(object):
    """ Definition of Node by which we can construct a tree. """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._parent = None

    @property
    def parent(self):
        return self._parent

    def set_parent(self, parent):
        self._parent = parent

    def is_root(self):
        return self.parent is None

    @property
    def leaf_nodes(self):
        return [self]

    def node_accept(self, visitor):
        visitor(self)


class CompositeNode(Node):
    """ Definition of CompositeNode who contains multiple child nodes. """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._children = []

    @property
    def child_nodes(self):
        return self._children

    def add_child(self, child):
        child.set_parent(self)
        self._children.append(child)

    def remove_child(self, child):
        child.set_parent(None)
        self._children.remove(child)

    @property
    def leaf_nodes(self):
        if len(self.child_nodes) == 0:
            return [self]
        nodes = []
        for child in self.child_nodes:
            nodes.extend(child.leaf_nodes)
        return nodes

    def dump(self):
        def _(node, indent):
            spaces = '  ' * indent
            output = [spaces, str(node), '\n']
            if isinstance(node, CompositeNode):
                for child in node.child_nodes:
                    output.extend(_(child, indent+1))
            return output
        return ''.join(_(self, 0))

    def node_accept(self, visitor : typing.Callable[[Node], typing.Any]):
        # pre-order
        visitor(self)
        for child in self.child_nodes:
            child.node_accept(visitor)
