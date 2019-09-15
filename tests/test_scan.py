#!/usr/bin/env python

import unittest

from dremel.reader import scan
from dremel.field_graph import FieldGraphError
from .utils import create_test_storage


class ScanTest(unittest.TestCase):
    def setUp(self):
        self.storage = create_test_storage()

    def test_create(self):
        for values, fetch_level in scan(self.storage, ['doc_id', 'links.backward']):
            print(values, fetch_level)
        for values, fetch_level in scan(self.storage, ['doc_id', 'name.url', 'name.language.code']):
            print(values, fetch_level)

    def test_invalid_repeated_fields(self):
        with self.assertRaisesRegex(FieldGraphError, 'independently-repeated fields'):
            for values, fetch_level in scan(self.storage, ['name.url', 'links.backward']):
                print(values, fetch_level)
