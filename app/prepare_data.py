# Import necessary libraries for file sanitization and progress tracking

from pathvalidate import sanitize_filename
from tqdm import tqdm
from pyspark.sql import SparkSession


# Initialize a Spark session for data processing

spark = SparkSession.builder \
    .appName('data preparation') \
    .master("local") \
    .config("spark.sql.parquet.enableVectorizedReader", "true") \
    .getOrCreate()


# Read the parquet file and sample 1000 documents

df = spark.read.parquet("/b.parquet")
n = 1000
df = df.select(['id', 'title', 'text']).sample(fraction=100 * n / df.count(), seed=0).limit(n)


# Function to create a text file for each document

def create_doc(row):
    filename = "data/" + sanitize_filename(str(row['id']) + "_" + row['title']).replace(" ", "_") + ".txt"
    with open(filename, "w") as f:
        f.write(row['text'])


df.foreach(create_doc)

# Write the document data to a CSV file in HDFS
df.write.csv("/index/data", sep = "\t")