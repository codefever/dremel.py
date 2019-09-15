#!/usr/bin/env python

import collections
import typing

from google.protobuf.message import Message
from google.protobuf.descriptor import Descriptor

from dremel.field_graph import FieldNode
from dremel.writer import new_message_writer
from dremel.reader import FieldStorage, FieldReader, SchemaFieldDescriptor, ReadError


# simple way to bridge readers and writers
def create_simple_storage(desc: Descriptor, msgs: typing.Iterable[Message], fields=None) -> FieldStorage:
    writer = new_message_writer(desc, fields)
    cols = collections.defaultdict(list)

    for leaf in writer.leaf_nodes:
        def helper(node, r, d, v):
            cols[node.path].append((r, d, v))
        leaf.set_write_callback(helper)

    for msg in msgs:
        writer.write(msg)

    return SimpleFieldStorage(cols, writer.field_graph)


class SimpleFieldStorage(FieldStorage):
    def __init__(self, col_data, field_graph):
        super().__init__()
        self._col_data = col_data
        self._field_graph = field_graph

    def create_field_reader(self, field_path: str) -> FieldReader:
        field_node = self._field_graph.get_field(field_path)
        if field_path in self._col_data and field_node:
            return SimpleFieldReader(self._col_data[field_path], field_node)
        return None

    def list_fields(self) -> typing.List[str]:
        return list(self._col_data.keys())

    @property
    def field_graph(self):
        return self._field_graph


class SimpleFieldReader(FieldReader):
    def __init__(self, col, node):
        super().__init__()
        self._col = col
        self._node = node
        self._pos = -1  # need an initial fetch()/next()

    @property
    def descriptor(self) -> SchemaFieldDescriptor:
        return self._node.descriptor

    @property
    def field_node(self) -> FieldNode:
        return self._node

    def repetition_level(self) -> int:
        if not self.done():
            self._check_pos()
            return self._col[self._pos][0]
        return 0

    def next_repetition_level(self) -> int:
        if self._pos + 1 < len(self._col):
            return self._col[self._pos + 1][0]
        return 0

    def definition_level(self) -> int:
        if not self.done():
            self._check_pos()
            return self._col[self._pos][1]
        return 0

    def value(self) -> typing.Any:
        if not self.done():
            self._check_pos()
            return self._col[self._pos][2]
        return None

    def done(self) -> bool:
        return self._pos >= len(self._col)

    def next(self) -> None:
        if not self.done():
            self._pos += 1

    def _check_pos(self):
        if self._pos == -1:
            raise ReadError('No initial fetch already')
