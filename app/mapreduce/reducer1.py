#!/usr/bin/env python3

import sys
from collections import defaultdict

# Reducer script for building the document index

# Dictionary to store term frequencies
term_dict = defaultdict(set)

# Read each line from standard input
for line in sys.stdin:
    # Strip whitespace and split the line into term and document ID
    line = line.strip()
    term, doc_id = line.split('\t')
    
    # Add document ID to the set of documents for the term
    term_dict[term].add(doc_id)

# Output the term and list of document IDs
for term, doc_ids in term_dict.items():
    print(f"{term}\t{','.join(doc_ids)}")



