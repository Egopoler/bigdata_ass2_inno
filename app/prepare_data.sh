#!/bin/bash

# Activate the Python virtual environment
source .venv/bin/activate

# Set the Python executable for PySpark driver
export PYSPARK_DRIVER_PYTHON=$(which python) 

# Unset PYSPARK_PYTHON to use the default Python
unset PYSPARK_PYTHON

# Upload the parquet file to HDFS
hdfs dfs -put -f b.parquet / && \
    # Run the data preparation script
    spark-submit prepare_data.py && \
    echo "Putting data to hdfs" && \
    # Upload the prepared data to HDFS
    hdfs dfs -put data / && \
    hdfs dfs -ls /data && \
    hdfs dfs -ls /index/data && \
    echo "done data preparation!"