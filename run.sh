# Copyright 2009 Javier Tordable.
# Author: jt@javiertordable.com (Javier Tordable).

# Runs the MapReduce Integer Factorization program.

# NOTE: Please edit these file paths and output directories.

# Path to the hadoop binary.
HADOOP_BIN="hadoop-0.20.1/bin/hadoop"

# Output directory for the results.
OUTPUT="output"

echo "Cleaning the output directory..."
$HADOOP_BIN dfs -rmr $OUTPUT

echo "Running mapreduction..."
$HADOOP_BIN jar mrif.jar \
    com.javiertordable.mrif.MapReduceQuadraticSieve \
    $1  # Integer to factor

echo "Output of the mapreduce:"
$HADOOP_BIN dfs -cat $OUTPUT/*
