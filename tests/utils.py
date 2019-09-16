#!/usr/bin/env python

import os
import random

from google.protobuf import text_format
from google.protobuf.descriptor import Descriptor, FieldDescriptor

from .document_pb2 import Document
from dremel.consts import *
from dremel.simple import create_simple_storage

def read_docs():
    sample_dir = os.path.join(os.path.dirname(__file__), 'samples')
    files = os.listdir(sample_dir)
    for f in files:
        with open(os.path.join(sample_dir, f)) as fd:
            doc = Document()
            text_format.Merge(fd.read(), doc)
            yield doc

def trim_doc(doc, fields):
    paths = {ROOT}
    for f in fields:
        p = ROOT
        for seg in f.split('.'):
            p += '.' + seg
            paths.add(p)

    def _trim(msg, root):
        for f,v in msg.ListFields():
            p = f'{root}.{f.name}'
            if p not in paths:
                msg.ClearField(f.name)
                continue
            if f.type in (FieldDescriptor.TYPE_GROUP, FieldDescriptor.TYPE_MESSAGE):
                if f.label == FieldDescriptor.LABEL_REPEATED:
                    for e in v: _trim(e, p)
                else:
                    _trim(v, p)
        return msg
    return _trim(doc, ROOT)

def _random_string(n):
    chars = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789:/+_(*^%$#@!~<>?)'
    return ''.join([random.choice(chars) for _ in range(n)])

def create_random_doc():
    doc = Document()
    doc.doc_id = random.randint(0, 999999)

    # links
    if random.random() < 0.3:
        fcnt, bcnt = random.randint(0, 10), random.randint(0, 7)
        doc.links.forward.extend([random.randint(1000, 4000) for _ in range(fcnt)])
        doc.links.backward.extend([random.randint(5000, 7000) for _ in range(bcnt)])

    # name
    while random.random() < 0.777:
        name = doc.name.add()
        while random.random() < 0.5:
            language = name.language.add()
            language.code = _random_string(6)
            if random.random() < 0.5:
                language.country = _random_string(9)
        if random.random() < 0.8:
            name.url = _random_string(12)

    return doc


def create_test_storage():
    return create_simple_storage(Document.DESCRIPTOR, read_docs())
