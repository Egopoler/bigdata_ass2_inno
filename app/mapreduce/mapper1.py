#!/usr/bin/env python3

import sys

# Mapper script for indexing documents

# Read each line from standard input
for line in sys.stdin:
    # Strip whitespace and split the line into components
    line = line.strip()
    doc_id, doc_title, doc_text = line.split('\t')
    
    # Emit terms with document ID
    for term in doc_text.split():
        print(f"{term}\t{doc_id}")
