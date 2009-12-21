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
