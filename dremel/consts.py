#!/usr/bin/env python

# Fake root node in field paths so as to replace the root node named by
# messages in `FieldDescriptor.full_name`.
# eg, `Document.doc_id` -> `{ROOT}.doc_id`
ROOT = '__root__'
