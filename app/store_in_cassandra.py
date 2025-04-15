#!/usr/bin/env python3

from cassandra.cluster import Cluster
import sys

# Connect to the Cassandra cluster
cluster = Cluster(['127.0.0.1'])  # Update with your Cassandra node IP
session = cluster.connect()

# Create keyspace and tables if they do not exist
session.execute("""
CREATE KEYSPACE IF NOT EXISTS search_index WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}
""")
session.set_keyspace('search_index')

session.execute("""
CREATE TABLE IF NOT EXISTS term_index (
    term text PRIMARY KEY,
    doc_ids set<text>
)
""")

# Read MapReduce output from standard input
for line in sys.stdin:
    # Strip whitespace and split the line into term and document IDs
    line = line.strip()
    term, doc_ids_str = line.split('\t')
    doc_ids = set(doc_ids_str.split(','))
    
    # Insert data into Cassandra table
    session.execute(
        "INSERT INTO term_index (term, doc_ids) VALUES (%s, %s)",
        (term, doc_ids)
    )

# Close the connection
cluster.shutdown() 



