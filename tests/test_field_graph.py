#!/usr/bin/env python

import unittest

from google.protobuf import text_format
from google.protobuf.descriptor import *

from dremel.schema_pb2 import Schema, SchemaFieldDescriptor, SchemaFieldGraph
from dremel.field_graph import FieldGraph, create_field_graph


class FieldGraphTest(unittest.TestCase):
    def test_create(self):
        text = '''field_descriptor {
  path: '__root__'
  cpp_type: 10
  label: 1
  max_repetition_level: 0
  definition_level: 0
}
field_descriptor {
  path: '__root__.a'
  cpp_type: 1
  label: 3
  max_repetition_level: 1
  definition_level: 1
}
field_graph {
    edge {
        from_field: '__root__'
        to_fields: '__root__.a'
    }
}
'''

        schema = Schema()
        text_format.Merge(text, schema)
        graph = create_field_graph(schema)
        self.assertSetEqual(set(['__root__','__root__.a']),
                            set([f.descriptor.path for f in graph.list_fields()]))
        self.assertTrue(graph.get_field('__root__'))
        self.assertTrue(graph.get_field('__root__.a'))
        self.assertFalse(graph.get_field('__root__.b'))
