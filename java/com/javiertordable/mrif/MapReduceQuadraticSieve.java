// Copyright 2009. Javier Tordable.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy
// of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.javiertordable.mrif;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigInteger;
import java.util.logging.Logger;

/**
 * This is the main class for the MapReduce for Integer factorization, using
 * the quadratic sieve algorithm.
 */
public class MapReduceQuadraticSieve extends Configured implements Tool {

  // Logger for the MapReduce class.
  private static final Logger LOGGER = Logger.getLogger(
      "MapReduceQuadraticSieve");

  /**
   * Each map input will be a sieve interval of the given size.
   *
   * It is currently set to 10 for testing. Change to 1000000 for production
   * use. 1 million BigIntegers in ASCII ~ 12 - 15 MB.
   */
  public static final int MAP_INPUT_INTERVAL_SIZE = 10;
  
  /**
   * The name of the input file for the mapreduce.
   */
  public static final String INPUT_FILE_NAME = "input_file";

  /**
   * Name of the MapRedce variable that holds the integer to factor.
   */
  public static final String INTEGER_TO_FACTOR_NAME = "N";

  /**
   * Name of the MapReduce variable that holds the factor base.
   */
  public static final String FACTOR_BASE_NAME = "FactorBase";

  /**
   * Setup the MapReduce parameters and run it.
   *
   * Tool parses the command line arguments for us.
   */
  public int run(String[] args) throws Exception {
    Configuration conf = getConf();

    // Check the arguments. we need the integer to attempt to factor.
    if (args.length < 1) {
      System.out.println("Please indicate the integer to factor");
      LOGGER.severe("No integer to factor. Exit.");
      System.exit(1);
    }

    // Parse N and add it to the job configuration, so that the workers can
    // access it as well.
    BigInteger N = new BigInteger(args[0]);
    LOGGER.info("Attempting factorization of: " + N.toString());
    conf.set(INTEGER_TO_FACTOR_NAME, N.toString());

    // Obtain the factor base for the integer N.
    FactorBaseArray factorBase = SieveInput.factorBase(N);
    LOGGER.info("Factor base of size: " + factorBase.size());
    conf.set(FACTOR_BASE_NAME, factorBase.toString());

    // Prepare the input of the mapreduce.
    LOGGER.info("Sieve of size: " + SieveInput.fullSieveIntervalSize(N));
    try {
        // Write the full sieve interval to disk.
	SieveInput.writeFullSieveInterval(N, "input/" + INPUT_FILE_NAME);
    } catch (FileNotFoundException e) {
      System.out.println("Unable to open the file for writing.");
    } catch (IOException e) {
      System.out.println("Unable to write to the output file.");
    }

    // Configure the classes of the mapreducer
    Job job = new Job(conf, "QuadraticSieve");
    job.setJarByClass(MapReduceQuadraticSieve.class);
    job.setMapperClass(SieveMapper.class);
    job.setReducerClass(FindSquaresReducer.class);

    // Output will be two pairs of strings:
    // <"Factor1", "59">
    // <"Factor2", "101">
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(job, new Path("input/"));
    FileOutputFormat.setOutputPath(job, new Path("output/"));

    // Submit the job.
    job.waitForCompletion(true);

    return 0;
  }

  /**
   * Run the MapReduce.
   */
  public static void main(String[] args) throws Exception {
    // ToolRunner will parse common command line options
    int result = ToolRunner.run(new Configuration(),
        new MapReduceQuadraticSieve(), args);
    System.exit(result);
  }
}
