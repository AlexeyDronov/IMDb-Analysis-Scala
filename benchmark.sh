#!/bin/bash

echo -e "Running Scala Spark script...\n"
sbt "runMain imdb.ImdbSpark $1 --verbose"
for i in {1..100}; do printf '-'; done; echo

echo -e "Running PySpark script...\n"
python python/imdb_pyspark.py --verbose --trials $1
exec bash