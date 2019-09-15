#!/usr/bin/env python

import typing

from dremel.consts import *
from dremel.field_graph import FieldGraph, FieldNode
from dremel.schema_pb2 import SchemaFieldDescriptor


class ReadError(Exception):
    pass


class FieldValueMixin(object):
    def __init__(self) -> None:
        super().__init__()

    @property
    def descriptor(self) -> SchemaFieldDescriptor:
        raise NotImplementedError()

    @property
    def field_node(self) -> FieldNode:
        raise NotImplementedError()

    def repetition_level(self) -> int:
        raise NotImplementedError()

    def next_repetition_level(self) -> int:
        raise NotImplementedError()

    def definition_level(self) -> int:
        raise NotImplementedError()

    def value(self) -> typing.Any:
        raise NotImplementedError()

    def __repr__(self) -> str:
        return f'<FieldValue:{self.descriptor.path}, R={self.repetition_level()}, NR={self.next_repetition_level()} D={self.definition_level()} V={self.value()}>'


class FieldReader(FieldValueMixin):
    def __init__(self) -> None:
        super().__init__()

    def done(self) -> bool:
        raise NotImplementedError()

    def next(self) -> None:
        raise NotImplementedError()


class FieldReaderSet(object):
    """ Wrap `Fetch` method in Appendix.D """
    def __init__(self) -> None:
        super().__init__()
        self._field_readers = []

    def add(self, field_reader: FieldReader) -> None:
        self._field_readers.append(field_reader)

    @property
    def field_readers(self) -> typing.List[FieldReader]:
        return self._field_readers

    def done(self) -> bool:
        return all([f.done() for f in self._field_readers])

    def fetch(self, fetch_level: int) -> typing.Tuple[int, bool]:
        next_level = 0
        all_done = True
        for f in self._field_readers:
            if f.next_repetition_level() >= fetch_level:
                f.next()
                next_level = max(next_level, f.next_repetition_level())
            if not f.done():
                all_done = False
        return next_level, all_done


class FieldStorage(object):
    def __init__(self) -> None:
        pass

    def create_field_reader(self, field_path: str) -> FieldReader:
        raise NotImplementedError()

    def list_fields(self) -> typing.List[str]:
        raise NotImplementedError()

    @property
    def field_graph(self):
        raise NotImplementedError()


def scan(storage: FieldStorage, project_fields: typing.List[str]) ->\
    typing.Generator[typing.Tuple[typing.List[typing.Any], int], None, None]:
    """ Simple prejections """
    field_reader_set = FieldReaderSet()
    for f in project_fields:
        reader = storage.create_field_reader(f'{ROOT}.{f}')
        if reader is None:
            raise ReadError(f'No field named "{f}"')
        field_reader_set.add(reader)

    # check if any independently repeated fields?
    storage.field_graph.check_if_independently_repeated_fields(
        [f.descriptor.path for f in field_reader_set.field_readers])

    values = [None for _ in range(len(project_fields))]
    fetch_level = 0

    while True:
        next_level, done = field_reader_set.fetch(fetch_level)
        if done:
            # nothing to iterate
            break

        for i, reader in enumerate(field_reader_set.field_readers):
            if reader.repetition_level() >= fetch_level:
                values[i] = reader.value()

        # Emit projection
        yield values, fetch_level
        fetch_level = next_level
