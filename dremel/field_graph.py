#!/usr/bin/env python

import collections
import typing
from google.protobuf.descriptor import *

from dremel.consts import *
from dremel.node import Node, CompositeNode
from dremel.schema_pb2 import Schema, SchemaFieldDescriptor, SchemaFieldGraph


class FieldGraphError(Exception):
    pass


class FieldNode(CompositeNode):
    """ FieldNode contributes to FieldGraph. """
    def __init__(self, descriptor : SchemaFieldDescriptor):
        super().__init__()
        self._descriptor = descriptor
        self._field_index = None

    @property
    def descriptor(self) -> SchemaFieldDescriptor:
        return self._descriptor

    @property
    def name(self) -> str:
        return self._descriptor.path.split('.')[-1]

    @property
    def field_index(self) -> int:
        return self._field_index

    def set_field_index(self, index: int):
        assert self._field_index is None and self.is_leaf()
        self._field_index = index

    def is_leaf(self):
        # BUG(me): root error?
        return self._descriptor.cpp_type not in (
            FieldDescriptor.TYPE_GROUP, FieldDescriptor.TYPE_MESSAGE)

    def __repr__(self):
        r = self.descriptor.max_repetition_level
        d = self.descriptor.definition_level
        return f'<FieldNode:{self.descriptor.path} leaf:{self.is_leaf()} R={r}, D={d}>'

    def lowest_common_ancestor_node_with(self, other):
        a = self._get_path_to_root()[::-1]
        b = other._get_path_to_root()[::-1]
        if a[0] != b[0]:
            raise FieldGraphError('Nodes from different graph.')
        common = a[0]
        for i in range(1, min(len(a), len(b))):
            if a[i] != b[i]: break
            common = a[i]
        return common

    def common_repetition_level_with(self, other):
        return self.lowest_common_ancestor_node_with(other).descriptor.max_repetition_level

    def _get_path_to_root(self):
        return self.get_path_to(None)

    def get_path_to(self, target):
        nodes = []
        current = self
        while current and current != target:
            nodes.append(current)
            current = current.parent
        if target is not None and current != target:
            raise FieldGraphError(f'No path to target: {target}')
        return nodes

class FieldGraph(object):
    """ FieldGraph remove the dependency of g_pb.Descriptor. """
    def __init__(self, root):
        self._root = root
        self._fields = dict()
        def _(f): self._fields[f.descriptor.path] = f
        self._root.node_accept(_)
        for i, node in enumerate(self._root.leaf_nodes):
            node.set_field_index(i)

    @property
    def root(self):
        return self._root

    def list_fields(self):
        return list(self._fields.values())

    def get_field(self, name) -> FieldNode:
        return self._fields.get(name)

    def dump(self) -> str:
        return self.root.dump()

    def to_field_graph(self) -> SchemaFieldGraph:
        graph = SchemaFieldGraph()
        def _(node):
            if not node.is_leaf():
                edge = SchemaFieldGraph.Edge()
                edge.from_field = node.descriptor.path
                edge.to_fields = [c.descriptor.path for c in node.child_nodes]
                graph.edges.add(edge)
        self._root.node_accept(_)
        return graph

    def check_if_independently_repeated_fields(self, fields: typing.List[str]):
        level_to_nodes = dict()

        for field in fields:
            field_node = self.get_field(field)

            current_level = field_node.descriptor.max_repetition_level
            current = field_node
            while (current.parent is not None and
                   current.parent.descriptor.max_repetition_level == current_level):
                current = current.parent

            if current_level in level_to_nodes and level_to_nodes[current_level][0] != current:
                raise FieldGraphError(f'Found multiple independently-repeated fields: \
{field} (from {current.descriptor.path}) and \
{level_to_nodes[current_level][1]} \
(from {level_to_nodes[current_level][0].descriptor.path}).')
            else:
                level_to_nodes[current_level] = (current, field)


def create_field_graph(graph: Schema) -> FieldGraph:
    field_map = dict((f.path, f) for f in graph.field_descriptor)
    links = dict((edge.from_field, edge.to_fields[:]) for edge in graph.field_graph.edge)

    seen_nodes = set()
    def create_node(field_name):
        if field_name in seen_nodes:
            raise FieldGraphError(f'Duplicate node in field graph: {field_name}')
        seen_nodes.add(field_name)

        link = links.get(field_name)
        field_desc = field_map.get(field_name)
        if field_desc is None:
            raise FieldGraphError(f'Missing field: {field_name}')

        current = FieldNode(field_desc)
        if not current.is_leaf():
            if link is None:
                raise FieldGraphError(f'No link found for a unleaf node: {field_name}')
        else:
            if link is not None:
                raise FieldGraphError(f'Found link for a leaf node: {field_name}')
        if link:
            for child in [create_node(f) for f in link]:
                current.add_child(child)
        return current

    root = create_node(ROOT)
    return FieldGraph(root)
