#!/usr/bin/env python

import os
from google.protobuf import text_format

from .document_pb2 import Document
from dremel.simple import create_simple_storage

def read_docs():
    sample_dir = os.path.join(os.path.dirname(__file__), 'samples')
    files = os.listdir(sample_dir)
    for f in files:
        with open(os.path.join(sample_dir, f)) as fd:
            doc = Document()
            text_format.Merge(fd.read(), doc)
            yield doc

def create_test_storage():
    return create_simple_storage(Document.DESCRIPTOR, read_docs())
