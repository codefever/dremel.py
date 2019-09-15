# Dremel.py

paper: https://static.googleusercontent.com/media/research.google.com/zh-CN//pubs/archive/36632.pdf


## Installation
```bash
pip install setup.py
```

## Test
```bash
python -m unittest discover -t .
```

## Usage

### Dissect records
```python
from collections import defaultdict
from dremel import writer
from document_pb2 import Document

# create writer
w = new_message_writer(Document().DESCRIPTOR, fields)

# install custom functions to store RDV data.
cols = defaultdict(list)
for leaf in w.leaf_nodes:
    leaf.set_write_callback(
        lambda node, r, d, v: cols[node.path].append((v, r, d)))

# load messages from somewhere
msgs = [...]

# dissect them
for msg in msgs:
    w.write(doc)
```

See also: `tests/test_writer.py`.

### Scan/Projection
There's also a simple bridge which provides an implementation for RDV storage.

```python
from dremel import simple, reader
from document_pb2 import Document

# create storage by the bridge
msgs = [...]
storage = simple.create_simple_storage(Document.DESCRIPTOR, msgs)
# or create your own storage

# scan!
for values, _ in reader.scan(storage, ['doc_id', 'name.url', 'name.language.code']):
    # do sth
    pass
```

See also: `tests/test_scan.py`.

### Assembly
```python
from dremel import assembly
from document_pb2 import Document

# create your own storage
storage = ...

# assemble!
builder = assembly.MessageAssemblyBuilder(storage.field_graph, Document)
assembly.assemble(storage, builder)
msgs = builder.get_msgs()  # <-results
```
See also: `tests/test_assembly.py`.
