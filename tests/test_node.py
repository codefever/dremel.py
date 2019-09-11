#!/usr/bin/env python

import unittest

from dremel.node import Node, CompositeNode


class NodeTest(unittest.TestCase):
    def test_nodes(self):
        root = CompositeNode()
        node1 = Node()
        root.add_child(node1)
        root2 = CompositeNode()
        root.add_child(root2)
        node2 = Node()
        root2.add_child(node2)
        root3 = CompositeNode()
        root2.add_child(root3)
        print(root.dump())

        # parents
        for node in [root, root2]:
            for child in node.child_nodes:
                self.assertEqual(node, child.parent)
        # leaves
        self.assertEqual([node1, node2, root3], root.leaf_nodes)
        self.assertEqual([node2, root3], root2.leaf_nodes)
        self.assertEqual([node1, root2], root.child_nodes)

        # traverse
        nodes = set()
        root.node_accept(lambda n: nodes.add(n))
        self.assertEqual(set([root, node1, node2, root2, root3]), nodes)
