#!/usr/bin/env python

import logging
import random
import unittest

from .document_pb2 import Document
from dremel.consts import *
from dremel.writer import new_message_writer
from dremel.assembly import MessageAssemblyBuilder, assemble, construct_fsm
from dremel.simple import create_simple_storage
from .utils import create_test_storage, read_docs, create_random_doc, trim_doc

#logging.basicConfig(level=logging.DEBUG)


class AssemblyTest(unittest.TestCase):
    def test_fsm(self):
        writer = new_message_writer(Document.DESCRIPTOR)
        field_graph = writer.field_graph
        #fsm, _ = construct_fsm(field_graph, fields=['doc_id', 'name.language.country'])
        fsm, _ = construct_fsm(field_graph, fields=None)
        for k,v in fsm.items():
            print(k, '=>', v)

    def test_assembly(self):
        docs = list(read_docs())
        storage = create_simple_storage(Document.DESCRIPTOR, docs)
        builder = MessageAssemblyBuilder(storage.field_graph, Document)
        assemble(storage, builder)
        msgs = builder.get_msgs()
        self.assertEqual(2, len(msgs))
        self.assertEqual(str(docs[0]), str(msgs[0]))
        self.assertEqual(str(docs[1]), str(msgs[1]))

    def test_assembly_for_arbitary_fields(self):
        docs = list(read_docs())
        fields = ['doc_id', 'links.backward', 'name.language.code']
        storage = create_simple_storage(Document.DESCRIPTOR, docs, fields= fields)
        builder = MessageAssemblyBuilder(storage.field_graph, Document)
        assemble(storage, builder)
        msgs = builder.get_msgs()
        self.assertEqual(2, len(msgs))
        for i, msg in enumerate(msgs):
            self.assertEqual(str(trim_doc(docs[i], fields)), str(msg))

    def test_ramdom_documents(self):
        docs = [create_random_doc() for i in range(100)]
        storage = create_simple_storage(Document.DESCRIPTOR, docs)
        builder = MessageAssemblyBuilder(storage.field_graph, Document)
        assemble(storage, builder)
        msgs = builder.get_msgs()
        self.assertEqual(len(docs), len(msgs))
        for i, msg in enumerate(msgs):
            self.assertEqual(str(docs[i]), str(msg))

    def test_ramdom_documents_for_arbitary_fields(self):
        docs = [create_random_doc() for i in range(100)]
        storage = create_simple_storage(Document.DESCRIPTOR, docs)

        all_fields = [f.descriptor.path.lstrip('.'+ROOT) for f in storage.field_graph.root.leaf_nodes]
        random.shuffle(all_fields)
        fields = all_fields[:random.randint(1, len(all_fields))]
        print('Fields:', fields)

        builder = MessageAssemblyBuilder(storage.field_graph, Document)
        assemble(storage, builder, fields)
        msgs = builder.get_msgs()
        self.assertEqual(len(docs), len(msgs))
        for i, msg in enumerate(msgs):
            self.assertEqual(str(trim_doc(docs[i], fields)), str(msg))
