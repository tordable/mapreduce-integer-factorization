// Copyright 2009. Javier Tordable.
// Author: jt@javiertordable.com (Javier Tordable).

package com.javiertordable.mrif;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import java.io.IOException;
import java.math.BigInteger;
import java.lang.InterruptedException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.logging.Logger;

/**
 * The Sieve Mapper performs the first step in the quadratic sieve
 * algorithm, that is, the sieve itself. It receives a series of pairs of the
 * form (i, i^2 - N) and returns those pairs such that the second factor is
 * smooth over a certain set of primes.
 *
 * Each input of the maper is a concrete set of pairs, which may be the whole
 * set that needs to be sieved or not. That is irrelevant to the algorithm and
 * is left to the input splitting algorithms of Hadoop MapReduce.
 *
 * All the outputs of the algorithm share the same key, as all of them will
 * have to be analyzed together by the reducer when attempting to find a
 * product of a combination of them that is a square.
 *
 * Input:
 *   Key (line number)   Value (sieve interval of fixed size)
 *   1                   2  -5734   5 -5703  7 -5670 11 -5635
 *   2                   13 -5598  17 -5559 31 -5518 37 -5475
 *   ...
 * Output:
 *   Key ("1")           Value (sieved interval of variable size)
 *   1                   2  -5734  11 -5635
 *   1                   17 -5559
 *   ...
 */
public class SieveMapper extends Mapper<LongWritable, Text, Text, Text> {

  // Logger for the Mapper class.
  private static final Logger LOGGER = Logger.getLogger("SieveMapper");

  // The integer to factor.
  private BigInteger N;

  // The factor base for the integer N.
  private FactorBaseArray factorBase;

  // Hadoop counters for error conditions.
  private Counter mapper_invalid_sieve_array;
  private Counter mapper_unable_to_output;

  /**
   * Computes the index i of the first integer in the SieveArray interval such
   * that p divides array.getEval(i) = array.getInt(i)^2 - N.
   * The sieve of the array will start then in the returned index.
   * Given the first index i, we know that the values of index i + k.p can
   * also be sieved. However there are two possible values of i, which are
   * not separated by a multiple of p. The value solutionIndex can be 0
   * or 1 to indicate which of these values to take.
   *
   * Getting this index is normally done with the Shanks-Tonelli algorithm.
   * That is, solving the equation x^2 - N = 0 (mod p) to get a value
   * x' such that p | x'^2 - N. Then compute the first element in the array
   * that is in x' + pZ. But we just proceed with trial division.
   *
   * @param N is the integer to factor.
   * @param p is a prime to use for sieving the array.
   * @param array is the array that will be sieved.
   * @param solutionIndex lets choose between start indices, should be either
   *     0 or 1.
   * @returns the first i, such that 0 <= i < array.size and
   *     p | array.getEval(i). Or -1 in case there is no such element.
   */
  public static int getFirstMultipleIndex(BigInteger N, BigInteger p,
      SieveArray array, int solutionIndex) {
    // Get the first evaluation that is divisible by p. This will be the
    // first solution.
    int result = -1;
    int i;
    for (i = 0; i < array.size(); i++) {
      if (array.getEval(i).remainder(p).equals(BigInteger.ZERO)) {
        result = i;
        break;
      }
    }

    // If we want the first solution, return the value we just obtained.
    if (solutionIndex == 0) {
      return result;
    }

    // Otherwise obtain the second solution and check that the diference
    // is not divisible by p. If it is divisible by p then we have only one
    // valid solution and we return the previous value.
    int secondResult = -1;
    int j;
    for (j = i + 1; j < array.size(); j++) {
      if (array.getEval(j).remainder(p).equals(BigInteger.ZERO)) {
        secondResult = j;
        break;
      }
    }
    BigInteger diffResults = BigInteger.valueOf(secondResult - result);
    if (diffResults.remainder(p).equals(BigInteger.ZERO)) {
      // Comes from the same solution.
      return result;
    } else {
      // Is a different solution.
      return secondResult;
    }
  }

  /**
   * Perform the sieve step in the quadratic sieve algorithm.
   * This method receives as input a SieveArray, composed of pairs of the form:
   * (i, i^2 - N), and it returns those pairs from the input set that  verify
   * that the element i^2 - N is smooth over the factor base.
   *
   * @param N is the integer to factor.
   * @param array is the input array to sieve.
   * @param factorBase is the factor base to check for smoothness.
   * @return A new {@link SieveArray} with the elements that are smooth over
   *     the factor base.
   */
  public static SieveArray sieve(BigInteger N, SieveArray array,
      FactorBaseArray factorBase) {
    // Initialize an auxiliary array for the quotients of sieved numbers.
    ArrayList<BigInteger> quotients = new ArrayList<BigInteger>(array.size());
    for (int i = 0; i < array.size(); i++) {
      quotients.add(array.getEval(i));
    }

    // For each prime in the factor base
    for (int i = 0; i < factorBase.size(); i++) {
      BigInteger p = factorBase.get(i);

      // Sieve for both solutions of x^2 - N = 0 (mod p).
      for (int numSolution = 0; numSolution < 2; numSolution++) {
        int sieveStart = getFirstMultipleIndex(N, p, array, numSolution);
        if (sieveStart == -1) {
          continue;
        }

        // Sieve for sieveStart + k * p
        // Take the intValue of p. If it doesn't fit in an int, then any
        // multiple of it won't be in the interval which has size < MAX_INT.
        for (int j = sieveStart; j < array.size(); j += p.intValue()) {
          BigInteger numberToSieve = quotients.get(j);

          while (numberToSieve.remainder(p).equals(BigInteger.ZERO)) {
            numberToSieve = numberToSieve.divide(p);
          }

          quotients.set(j, numberToSieve);
        }
      }
    }

    // Count the number of sieved elements. The quotient will be +1 or -1.
    int numResults = 0;
    for (int i = 0; i < quotients.size(); i++) {
      if (quotients.get(i).abs().equals(BigInteger.ONE)) {
        numResults++;
      }
    }

    // Create new arrays for the integers and evaluations of the sieved
    // elements, and move them to a new SieveArray.
    ArrayList<BigInteger> ints = new ArrayList<BigInteger>(numResults);
    ArrayList<BigInteger> evals = new ArrayList<BigInteger>(numResults);
    for (int i = 0; i < array.size(); i++) {
      if (quotients.get(i).abs().equals(BigInteger.ONE)) {
        ints.add(array.getInt(i));
        evals.add(array.getEval(i));
      }
    }
    SieveArray sieved = SieveArray.fromArrayLists(ints, evals);
    return sieved;
  }

  /**
   * Setup prepares the tasks common for all calls to the mapper. It retrieves
   * the integer to factor and the factor base from the job context.
   *
   * @param context is the Mapper context.
   */
  protected void setup(Context context) throws IOException {
    // Initialize counters.
    mapper_invalid_sieve_array = context.getCounter("SieveMapper",
        "invalid_sieve_array");
    mapper_unable_to_output = context.getCounter("SieveMapper",
        "unable_to_output");

    // Get the integer to factor and the factor base from the job configuration.
    N = new BigInteger(context.getConfiguration().get(
        MapReduceQuadraticSieve.INTEGER_TO_FACTOR_NAME));
    try {
      factorBase = FactorBaseArray.fromString(context.getConfiguration().get(
          MapReduceQuadraticSieve.FACTOR_BASE_NAME));
    } catch (ParseException e) {
      LOGGER.severe("Unable to read the factor base");
      throw new IOException(e);
    }
  }

  /**
   * Perform the map phase of the MapReduce.
   *
   * @param key is the input key, the line number of the input file. Ignored.
   * @param value is a serialized {@link SieveArray}.
   * @param context is the Mapper context, with common information for all the
   *     mapper jobs.
   */
  public void map(LongWritable key, Text value, Context context) {
    // Extract the sieve interval from the input.
    SieveArray sieveInterval;
    try {
      sieveInterval = SieveArray.fromString(value.toString(), 0);
    } catch (ParseException e) {
      mapper_invalid_sieve_array.increment(1);
      LOGGER.severe("Unable to parse the input SieveArray. Exiting.");
      LOGGER.info(value.toString());
      return;
    }

    // Sieve
    SieveArray sieved = sieve(N, sieveInterval, factorBase);

    // Output the sieved results
    Text outputKey = new Text("1");
    Text outputValue = new Text(sieved.toString());
    try {
      context.write(outputKey, outputValue);
    } catch (IOException e) {
      mapper_unable_to_output.increment(1);
      LOGGER.severe("Unable to write the map output. IOException: " + e);
      return;
    } catch (InterruptedException e) {
      mapper_unable_to_output.increment(1);
      LOGGER.severe(
          "Unable to write the map output. InterruptedException: " + e);
      return;
    }
  }
}
