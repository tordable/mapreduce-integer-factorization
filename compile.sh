# Copyright 2009 Javier Tordable.
# Author: jt@javiertordable.com (Javier Tordable).

# Compiles the MapReduce Integer Factorization program.

# NOTE: Please edit these file paths and destination directories.

# Path to the hadoop core jar.
HADOOP_JAR="hadoop-0.20.1/hadoop-0.20.1-core.jar"

# Destination directory for the classes. Must exist previously.
DESTINATION="bin/"

echo "Compiling classes..."
javac -classpath $HADOOP_JAR -d $DESTINATION -Xlint:deprecation \
    java/com/javiertordable/mrif/*.java

echo "Packing mrif.jar..."
jar -cvf mrif.jar -C bin/ .
