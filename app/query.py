from pyspark.sql import SparkSession
from cassandra.cluster import Cluster
import sys
import math

def calculate_bm25(query_terms, doc_id, doc_text, avg_doc_length, total_docs, doc_freqs, k1=1.5, b=0.75):
    # Calculate BM25 score for a document
    score = 0.0
    doc_length = len(doc_text.split())
    for term in query_terms:
        tf = doc_text.split().count(term)
        df = doc_freqs.get(term, 0)
        idf = math.log((total_docs - df + 0.5) / (df + 0.5) + 1)
        score += idf * ((tf * (k1 + 1)) / (tf + k1 * (1 - b + b * (doc_length / avg_doc_length))))
    return score

def main():
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("BM25 Query") \
        .getOrCreate()

    # Connect to Cassandra
    cluster = Cluster(['127.0.0.1'])  # Update with your Cassandra node IP
    session = cluster.connect('search_index')

    # Read query from stdin
    query = sys.stdin.read().strip()
    query_terms = query.split()

    # Retrieve document index and vocabulary from Cassandra
    rows = session.execute("SELECT term, doc_ids FROM term_index")
    doc_freqs = {row.term: len(row.doc_ids) for row in rows}

    # Calculate average document length
    total_docs = len(doc_freqs)
    avg_doc_length = sum(len(doc_ids) for doc_ids in doc_freqs.values()) / total_docs

    # Calculate BM25 scores
    scores = []
    for row in rows:
        for doc_id in row.doc_ids:
            doc_text = ""  # Retrieve document text from your data source
            score = calculate_bm25(query_terms, doc_id, doc_text, avg_doc_length, total_docs, doc_freqs)
            scores.append((doc_id, score))

    # Sort and retrieve top 10 documents
    top_docs = sorted(scores, key=lambda x: x[1], reverse=True)[:10]

    # Display results
    for doc_id, score in top_docs:
        print(f"Document ID: {doc_id}, Score: {score}")

    # Close connections
    cluster.shutdown()
    spark.stop()

if __name__ == "__main__":
    main()