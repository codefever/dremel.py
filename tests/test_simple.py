#!/usr/bin/env python

import os
import unittest

from google.protobuf import text_format

from .document_pb2 import *
from dremel.simple import create_simple_storage
from .test_writer import (DOCID, LINKS_BACKWARD, LINKS_FORWARD, NAME_URL,
                          NAME_LANGUAGE_CODE, NAME_LANGUAGE_COUNTRY)


def read_docs():
    sample_dir = os.path.join(os.path.dirname(__file__), 'samples')
    files = os.listdir(sample_dir)
    for f in files:
        with open(os.path.join(sample_dir, f)) as fd:
            doc = Document()
            text_format.Merge(fd.read(), doc)
            yield doc

def to_rdv(field_reader):
    ret = []
    next_r = field_reader.next_repetition_level()
    field_reader.next()
    while not field_reader.done():
        r = field_reader.repetition_level()
        nr = field_reader.next_repetition_level()
        d = field_reader.definition_level()
        assert next_r == r
        next_r = nr
        ret.append((field_reader.value(), r, d))
        field_reader.next()
    return ret


class SimpleBridgeTest(unittest.TestCase):
    def test_create(self):
        storage = create_simple_storage(Document.DESCRIPTOR, read_docs())
        self.assertIsNotNone(storage.field_graph)
        self.assertIsNotNone(storage.create_field_reader('__root__.doc_id'))
        self.assertIsNone(storage.create_field_reader('__root__'))
        self.assertEqual(DOCID, to_rdv(storage.create_field_reader('__root__.doc_id')))
        self.assertEqual(LINKS_BACKWARD, to_rdv(storage.create_field_reader('__root__.links.backward')))
        self.assertEqual(LINKS_FORWARD, to_rdv(storage.create_field_reader('__root__.links.forward')))
        self.assertEqual(NAME_LANGUAGE_COUNTRY, to_rdv(storage.create_field_reader('__root__.name.language.country')))
        self.assertEqual(NAME_LANGUAGE_CODE, to_rdv(storage.create_field_reader('__root__.name.language.code')))
        self.assertEqual(NAME_URL, to_rdv(storage.create_field_reader('__root__.name.url')))
