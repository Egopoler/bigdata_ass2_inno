#!/bin/bash
echo "This script will include commands to search for documents given the query using Spark RDD"


source .venv/bin/activate

# Python of the driver (/app/.venv/bin/python)
export PYSPARK_DRIVER_PYTHON=$(which python) 

# Python of the excutor (./.venv/bin/python)
export PYSPARK_PYTHON=./.venv/bin/python


if [ -z "$1" ]; then
  echo "Usage: $0 <query>"
  exit 1
fi

# Run the PySpark application on YARN
spark-submit --master yarn --deploy-mode cluster --archives /app/.venv.tar.gz#.venv app/query.py <<< "$1"






