#!/bin/bash

# Indexing script to run MapReduce job and manage index data in Cassandra
# Check if a path is provided, otherwise use default HDFS path
DATA_PATH=${1:-/index/data}

# Run the Hadoop MapReduce job
hadoop jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    -input /index/data \
    -output /tmp/index \
    -mapper "python3 mapper1.py" \
    -reducer "python3 reducer1.py"

# Check if the MapReduce job was successful
if [ $? -eq 0 ]; then
    echo "MapReduce job completed successfully"
    # Process the output and store in Cassandra
    hadoop fs -cat /tmp/index/part-* | python3 store_in_cassandra.py
else
    echo "MapReduce job failed"
fi

echo "This script include commands to run mapreduce jobs using hadoop streaming to index documents"

echo "Input file is :"
echo $1


hdfs dfs -ls /