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

Installation
============

Before installing MapRedude Integer Factorization (MRIF) please make sure that
you have the JavaSDK installed. And check that the javac and jar tool are in
the $PATH.

1. Unpack MRIF into its own directory
2. Download Hadoop-0.20.* from:
   http://www.apache.org/dyn/closer.cgi/hadoop/core/
3. Unpack hadoop:
   tar zvxf hadoop-*.
   Remove the tar.gz if you wish after it's done.
4. Create a subdirectory for the javac output, for example:
   mkdir bin
5. Edit compile.sh and add the correct path to the Hadoop directory and main
   library. hadoop-version-core.jar. Also indicate the directory for the
   javac output from the previous step.
6. Run
   ./compile.sh
   It will compile all the classes and pack them in a file called:
   quadraticsieve.jar.
7. Create a directory for the mapreduce input called input/:
   mkdir input/
8. Create a directory for the mapreduce output called output/:
   mkdir output/
9. Edit run.sh to add the correct path to the hadoop binary,
   in hadoop_dir/bin/hadoop
10.Run the program, passing as a parameter the number to factor, for example:
   ./run.sh 5959

You may also want to take a look at MapReduceQuadraticSieve.java and modify
some of the MapReduce parameters in order to adjust the job to your production
requirements.

Notes
=====

I didn't include the unit tests in this public release, but if you want them
I can send them to you. Just contact me using the form at
www.javiertordable.com/contact.
