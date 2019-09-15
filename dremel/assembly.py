#!/usr/bin/env python

import logging
import typing
from google.protobuf.descriptor import FieldDescriptor

from dremel.node import Node
from dremel.consts import *
from dremel.field_graph import FieldGraph, FieldNode
from dremel.reader import FieldStorage, FieldValueMixin, FieldReader


class AssemblyError(Exception):
    pass


class AssemblyBuilder(object):
    def __init__(self):
        super().__init__()

    def start(self):
        raise NotImplementedError()

    def rollback(self):
        raise NotImplementedError()

    def done(self):
        raise NotImplementedError()

    def assign_value(self, field: FieldValueMixin):
        raise NotImplementedError()


class MessageAssemblyBuilder(AssemblyBuilder):
    def __init__(self, field_graph: FieldGraph, factory):
        super().__init__()
        self._field_graph = field_graph
        self._factory = factory
        self._stack = []
        self._msgs = []
        self._last_node = None

    def get_msgs(self):
        return self._msgs

    def start(self):
        logging.debug('start message.')
        assert len(self._stack) == 0
        self._stack = [(self._factory(), self._field_graph.root)]
        self._last_node = None

    def rollback(self):
        logging.debug('rollback message.')
        self._stack = []

    def done(self):
        logging.debug('finish message.')
        msg = self._stack[0][0]
        self._msgs.append(msg)
        self._stack = []

    def assign_value(self, field: FieldValueMixin):
        logging.debug(f'Move from: {self._stack[-1][1].descriptor.path} to: {field.field_node.descriptor.path}')
        logging.debug(f'Value: {field}')
        # Consider:
        #  message A {
        #    repeated B b = 1;
        #    message B {
        #      repeated int i = 1;
        #    }
        #  }

        # move up to level
        current_node = field.field_node
        barrier = current_node.lowest_common_ancestor_node_with(self._stack[-1][1])
        # When reverse links are found, some repetition levels should be restarted.
        if self._last_node is not None and current_node.field_index < self._last_node.field_index:
            while barrier != self._field_graph.root and barrier.descriptor.max_repetition_level >= field.repetition_level():
                barrier = barrier.parent
        logging.debug(f'Barrier: {barrier}')
        while self._stack[-1][1] != barrier:
            self._stack.pop()
        logging.debug(f'Up: {self._stack}')

        # then go down
        path = current_node.get_path_to(barrier)[::-1]
        while path and path[0].descriptor.definition_level <= field.definition_level():
            logging.debug(f'Down: path={path} stack_last={self._stack[-1]}')
            last = self._stack[-1][0]
            node, path = path[0], path[1:]
            if node.is_leaf():
                if node != current_node:
                    raise AssemblyError(f'Unexpected leaf node {node} before {current_node}')
                assert len(path) == 0, path
                # set value
                if node.descriptor.label == FieldDescriptor.LABEL_REPEATED:
                    #last.setdefault(node.name, []).append(field.value())
                    getattr(last, node.name).append(field.value())
                else:
                    #last[node.name] = field.value()
                    setattr(last, node.name, field.value())

                logging.debug(f'set last={last} root={self._stack[0][0]}')
            else:
                # create sub-messages
                msg = dict()
                if node.descriptor.label == FieldDescriptor.LABEL_REPEATED:
                    #last.setdefault(node.name, []).append(msg)
                    msg = getattr(last, node.name).add()
                else:
                    #last[node.name] = msg
                    getattr(last, node.name).SetInParent()
                    msg = getattr(last, node.name)
                self._stack.append((msg, node))

        self._last_node = current_node


def _dfs(graph: FieldGraph, fields=None):
    """ DFS but also preserve definition orders. """
    field_set = set([f'{ROOT}.{f}' for f in fields]) if fields else None
    return [node for node in graph.root.leaf_nodes if field_set is None or node.descriptor.path in field_set]


FSM = typing.Dict[typing.Tuple[FieldNode, int], FieldNode]


def construct_fsm(graph: FieldGraph, fields=None, end_node=None) -> typing.Tuple[FSM, typing.List[FieldNode]]:
    field_nodes = _dfs(graph, fields)
    states = dict()

    for i, current in enumerate(field_nodes):
        max_level = current.descriptor.max_repetition_level
        barrier = field_nodes[i+1] if i+1 < len(field_nodes) else end_node
        barrier_level = current.common_repetition_level_with(barrier) if barrier else 0

        pre_fields = [f for f in field_nodes[:i+1] if f.descriptor.max_repetition_level > barrier_level]
        for pre_field in pre_fields:
            back_level = current.common_repetition_level_with(pre_field)
            states.setdefault((current, back_level), pre_field)

        for level in range(barrier_level+1, max_level+1):
            if (current, level) not in states:
                states[(current, level)] = states[(current, level-1)]

        for level in range(0, barrier_level+1):
            states[(current, level)] = barrier

    return states, field_nodes

def assemble(storage: FieldStorage, builder: AssemblyBuilder, fields=None):
    fsm, field_nodes = construct_fsm(storage.field_graph, fields)
    readers = []
    for node in field_nodes:
        r = storage.create_field_reader(node.descriptor.path)
        if not r:
            raise AssemblyError(f'No such field {node.descriptor.path} in storage')
        readers.append(r)
    _assemble(fsm, readers, builder)

def _assemble(fsm: FSM, field_readers: typing.List[FieldReader], builder: AssemblyBuilder):
    reader_map = dict((f.descriptor.path, f) for f in field_readers)
    fsm_readers = dict(((reader_map[k[0].descriptor.path], k[1]), reader_map[v.descriptor.path] if v else None) for k,v in fsm.items())

    def _read_message():
        reader = field_readers[0]
        builder.start()
        while reader:
            reader.next()
            if reader.done():
                builder.rollback()
                return False

            # set values
            builder.assign_value(reader)

            # go to next reader
            reader = fsm_readers.get((reader, reader.next_repetition_level()))
        builder.done()
        return True

    while True:
        if not _read_message(): break
