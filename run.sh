# Copyright 2009 Javier Tordable.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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
