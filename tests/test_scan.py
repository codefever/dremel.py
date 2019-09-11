#!/usr/bin/env python

import os
import unittest

from google.protobuf import text_format

from .document_pb2 import *
from dremel.simple import create_simple_storage
from dremel.reader import scan
from dremel.field_graph import FieldGraphError


def read_docs():
    sample_dir = os.path.join(os.path.dirname(__file__), 'samples')
    files = os.listdir(sample_dir)
    for f in files:
        with open(os.path.join(sample_dir, f)) as fd:
            doc = Document()
            text_format.Merge(fd.read(), doc)
            yield doc


class ScanTest(unittest.TestCase):
    def setUp(self):
        self.storage = create_simple_storage(Document.DESCRIPTOR, read_docs())

    def test_create(self):
        for values, fetch_level in scan(self.storage, ['doc_id', 'links.backward']):
            print(values, fetch_level)
        for values, fetch_level in scan(self.storage, ['doc_id', 'name.url', 'name.language.code']):
            print(values, fetch_level)

    def test_invalid_repeated_fields(self):
        with self.assertRaisesRegex(FieldGraphError, 'independently-repeated fields'):
            for values, fetch_level in scan(self.storage, ['name.url', 'links.backward']):
                print(values, fetch_level)
