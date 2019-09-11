#!/usr/bin/env python

from collections import defaultdict
import os
import unittest

from google.protobuf import text_format

from .document_pb2 import *
from dremel.writer import new_message_writer


DOCID = [
    (10, 0, 0),
    (20, 0, 0),
]
NAME_URL = [
    ('http://A', 0, 2),
    ('http://B', 1, 2),
    (None, 1, 1),
    ('http://C', 0, 2),
]
LINKS_FORWARD = [
    (20, 0, 2),
    (40, 1, 2),
    (60, 1, 2),
    (80, 0, 2),
]
LINKS_BACKWARD = [
    (None, 0, 1),
    (10, 0, 2),
    (30, 1, 2),
]
NAME_LANGUAGE_CODE = [
    ('en-us', 0, 2),
    ('en', 2, 2),
    (None, 1, 1),
    ('en-gb', 1, 2),
    (None, 0, 1),
]
NAME_LANGUAGE_COUNTRY = [
    ('us', 0, 3),
    (None, 2, 2),
    (None, 1, 1),
    ('gb', 1, 3),
    (None, 0, 1),
]


class WriterTest(unittest.TestCase):
    def test_with_paper(self):
        fields = []
        writer = new_message_writer(Document().DESCRIPTOR, fields)

        print('Dump writers:\n', writer.dump())

        # Use a simple collector for column values
        cols = defaultdict(list)

        # install write funcs
        for leaf in writer.leaf_nodes:
            leaf.set_write_callback(
                lambda node, r, d, v: cols[node.path].append((v, r, d)))

        sample_dir = os.path.join(os.path.dirname(__file__), 'samples')
        files = os.listdir(sample_dir)
        for f in files:
            with open(os.path.join(sample_dir, f)) as fd:
                doc = Document()
                text_format.Merge(fd.read(), doc)
                writer.write(doc)

        self.assertEqual(DOCID, cols.get('__root__.doc_id'))
        self.assertEqual(NAME_URL, cols.get('__root__.name.url'))
        self.assertEqual(LINKS_FORWARD, cols.get('__root__.links.forward'))
        self.assertEqual(LINKS_BACKWARD, cols.get('__root__.links.backward'))
        self.assertEqual(NAME_LANGUAGE_CODE, cols.get('__root__.name.language.code'))
        self.assertEqual(NAME_LANGUAGE_COUNTRY, cols.get('__root__.name.language.country'))

        for k,v in cols.items():
            print('# ', k)
            for e in v:
                print(e)
            print('\n')

    def test_optional_fields(self):
        fields = ['doc_id', 'name.language.code', 'links']
        writer = new_message_writer(Document().DESCRIPTOR, fields)
        print('Dump writers:\n', writer.dump())
        self.assertEqual(['__root__.doc_id', '__root__.name.language.code'],
                         [node.path for node in writer.leaf_nodes])

        # Node of `links` should be present.
        paths = []
        def collect_paths(node):
            paths.append(node.path)

        writer.node_accept(collect_paths)
        self.assertFalse('__root__.links' in paths)

    def test_field_graph(self):
        writer = new_message_writer(Document().DESCRIPTOR)
        field_graph = writer.field_graph
        print(field_graph.dump())
