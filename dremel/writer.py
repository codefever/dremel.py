#!/usr/bin/env python

import logging
from google.protobuf.descriptor import Descriptor, FieldDescriptor

from dremel.consts import *
from dremel.node import Node, CompositeNode
from dremel.field_graph import FieldNode, FieldGraph
from dremel.schema_pb2 import Schema, SchemaFieldDescriptor, SchemaFieldGraph


class DissectError(Exception):
    """ Exception type in this package. """
    pass


class FieldMixin(object):
    def __init__(self, path, desc,
                 max_repetition_level=0,
                 definition_level=0):
        super().__init__()
        self._path = path
        self._desc = desc
        self._max_repetition_level = max_repetition_level
        self._definition_level = definition_level

    @property
    def path(self):
        return self._path

    @property
    def field_descriptor(self):
        return self._desc

    @property
    def max_repetition_level(self):
        return self._max_repetition_level

    @property
    def definition_level(self):
        return self._definition_level


class FieldWriter(FieldMixin, Node):
    def __init__(self, path, desc,
                 max_repetition_level=0,
                 definition_level=0,
                 write_callback=None):
        super().__init__(path, desc, max_repetition_level, definition_level)
        self._write_callback = write_callback

    def set_write_callback(self, callback):
        # setup custom callbacks
        self._write_callback = callback

    def __repr__(self):
        return f'<Field: {self.path} R={self.max_repetition_level} D={self.definition_level}>'

    def accept(self, r, d, msg, visitor):
        if msg is None:
            self._accept(r, d, None, visitor)
            return

        # NOTE(me): Here `msg` is the outer scope for values, by which in cpp
        # it would be more convenient to handle field type dispatching.
        label = self._desc.label
        field_name = self._desc.name
        if label == FieldDescriptor.LABEL_REQUIRED:
            assert msg.HasField(field_name), f"Missing required field: {field_name}"
            self._accept(r, d, getattr(msg, field_name), visitor)
        elif label == FieldDescriptor.LABEL_OPTIONAL:
            has_val = msg.HasField(field_name)
            local_d = d+1 if has_val else d
            val = getattr(msg, field_name) if has_val else None
            self._accept(r, local_d, val, visitor)
        elif label == FieldDescriptor.LABEL_REPEATED:
            vals = getattr(msg, field_name)
            if len(vals) == 0:
                self._accept(r, d, None, visitor)
            else:
                local_r = r
                for val in vals:
                    self._accept(local_r, d+1, val, visitor)
                    local_r = self._max_repetition_level
        else:
            raise DissectError("Invalid field label: {}".format(str(self._desc)))

    def _accept(self, r, d, v, visitor):
        visitor(self, r, d, v)


class MessageWriter(FieldWriter, CompositeNode):
    def __init__(self, path, desc,
                 max_repetition_level=0,
                 definition_level=0):
        super().__init__(path, desc, max_repetition_level, definition_level)
        self._field_graph = None

    def __repr__(self):
        return f'<Message: {self.path} R={self.max_repetition_level} D={self.definition_level}>'

    @property
    def field_graph(self):
        if self._field_graph is None:
            self._field_graph = self._init_field_graph()
        return self._field_graph

    def _init_field_graph(self):
        def _(node):
            desc = SchemaFieldDescriptor(
                path=node.path,
                cpp_type=node.field_descriptor.cpp_type if node.field_descriptor else None,
                label=node.field_descriptor.label if node.field_descriptor else None,
                max_repetition_level=node.max_repetition_level,
                definition_level=node.definition_level)
            current = FieldNode(desc)
            for child in getattr(node, 'child_nodes', []):
                child_node = _(child)
                current.add_child(child_node)
            return current

        root = _(self)
        return FieldGraph(root)

    def accept(self, r, d, msg, visitor):
        if self.is_root():
            # root msg has no outer scopes, so we should treat it specially.
            self._accept(r, d, msg, visitor)
        else:
            super().accept(r, d, msg, visitor)

    def _accept(self, r, d, v, visitor):
        for child in self.child_nodes:
            child.accept(r, d, v, visitor)

    def write(self, msg):
        if not self.is_root():
            raise DissectError('cannnot write from non root nodes')
        def visitor(node, r, d, v):
            if node._write_callback:
                node._write_callback(node, r, d, v)
        self.accept(0, 0, msg, visitor)


def _get_valid_paths(fields):
    """ Generate all possible field paths which are traversable. """
    if fields is None or len(fields) == 0:
        return None

    m = dict()
    for field in fields:
        current = ROOT
        segs = field.split('.')
        for i, seg in enumerate(segs):
            current += f'.{seg}'
            leaf = (i+1 == len(segs))
            if current in m:
                if m[current] != leaf:
                    raise DissectError(f'Found an intermediate node conflicted: {current}')
            else:
                m[current] = leaf
    return m

def _recurse_create_nodes(msg_desc, node, valid_paths, circuit_checks):
    """ Create nodes recursively. """
    if msg_desc.name in circuit_checks:
        raise DissectError(f'Found recursive message definition: {msg_desc.name}')
    circuit_checks.add(msg_desc.name)

    for field in msg_desc.fields:
        path = f'{node.path}.{field.name}'
        if valid_paths is not None and path not in valid_paths:
            logging.debug('invalid path: %s', path)
            continue
        max_repetition_level = node.max_repetition_level
        definition_level = node.definition_level
        if field.label == FieldDescriptor.LABEL_OPTIONAL:
            definition_level += 1
        elif field.label == FieldDescriptor.LABEL_REPEATED:
            definition_level += 1
            max_repetition_level += 1
        if field.type in (FieldDescriptor.TYPE_GROUP, FieldDescriptor.TYPE_MESSAGE):
            child = MessageWriter(path, field, max_repetition_level, definition_level)
            _recurse_create_nodes(field.message_type, child, valid_paths, circuit_checks)
        else:
            child = FieldWriter(path, field, max_repetition_level, definition_level)
            logging.debug('create field writer: %s', path)
        node.add_child(child)

    circuit_checks.remove(msg_desc.name)

def _prune(node):
    """ Remove unused message nodes. """
    while node.parent is not None:
        parent = node.parent
        parent.remove_child(node)
        logging.info('prune node: %s', node.path)
        if len(parent.child_nodes) == 0:
            node = parent
        else:
            break


def new_message_writer(msg_desc, fields=None):
    valid_paths = _get_valid_paths(fields)
    writer = MessageWriter(ROOT, None)
    _recurse_create_nodes(msg_desc, writer, valid_paths, set())

    # prune used nodes
    dead_nodes = []
    def _(node):
        if isinstance(node, MessageWriter) and len(node.child_nodes) == 0:
            dead_nodes.append(node)
    writer.node_accept(_)
    for node in dead_nodes:
        _prune(node)
    if len(writer.child_nodes) == 0:
        raise DissectError(f'No valid leaf fields in root writer, chosen: {fields}')
    return writer
