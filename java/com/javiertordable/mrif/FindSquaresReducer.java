// Copyright 2009. Javier Tordable.
// Author: jt@javiertordable.com (Javier Tordable).

package com.javiertordable.mrif;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import java.io.IOException;
import java.math.BigInteger;
import java.lang.InterruptedException;
import java.lang.Math;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.logging.Logger;

/**
 * The FindSquaresReducer performs the second step in the quadratic sieve
 * algorithm, finding a subset of the factors obtained after the sieve whose
 * product is a square, and using that to attempt the factorization of N.
 *
 * In order to find this square we obtain the decomposition into primes of
 * the factors given in the input. This is very fast because we know that
 * they are smooth over the factor base. This is indeed the reason for the
 * sieve itself.
 *
 * From the decomposition modulo 2 we build a matrix. A solution of the system
 * with this matrix leads to a number wich verifies that all exponents of all
 * primes in its decomposition are even or zero mod 2, ergo its a square.
 *
 * This square is used to attempt the factorization of N. If it is succesful
 * we return the factors and exit, otherwise we attempt to find another
 * square. The more factors that we had initially the more options for finding
 * this square and thus the bigger the probability of finding a factorization
 * for N.
 *
 * Input:
 *   Key ("1")           Value (sieved interval of variable size)
 *   1                   2  -5734  11 -5635
 *   2                   17 -5559
 *   ...
 * Output:
 *   Key                 Value (factor of N)
 *   Factor1             59
 *   Factor2             101
 */
public class FindSquaresReducer extends Reducer<Text, Text, Text, Text> {

  // Logger for the Reducer class.
  private static final Logger LOGGER = Logger.getLogger("FindSquaresReducer");

  // The integer to factor.
  private BigInteger N;

  // The factor base for the integer N.
  private FactorBaseArray factorBase;

  // Hadoop counters for error conditions.
  private Counter reducer_invalid_sieve_array;
  private Counter reducer_unable_to_output;
  private Counter reducer_unable_to_solve_system;
  private Counter reducer_cant_factor;

  // Maximum number of attempts to fnd a square in the solution of the system.
  private static final int MAX_FIND_SQUARE_ATTEMPTS = 1000000;

  /**
   * Factors the integer given using the primes in the factor base.
   * It returns an array with the exponents of each one of the primes
   * in the factor base in the decomposition of the integer.
   *
   * There are many possible optimizations for this method, however
   * we will just stick with a basic factorization for simplicity.
   *
   * @param a is the integer to decompose, which is smooth over factorBase.
   * @param factorBase is a set of integers to use for the decomposition.
   * @returns An array of the same size as factorBase with the exponents of
   *     those primes in the decomposition of a.
   */
  public static int[] smoothFactor(BigInteger a, FactorBaseArray factorBase) {
    int[] exponents = new int[factorBase.size()];
    for (int i = 0; i < exponents.length; i++) {
      exponents[i] = 0;
    }

    // For each factor in the factor base.
    for (int i = 0; i < factorBase.size(); i++) {
      BigInteger factor = factorBase.get(i);

      // Get the maximum exponent such that facor^j divides a.
      int j = 1;
      while (a.remainder(factor.pow(j)).equals(BigInteger.ZERO)) {
        exponents[i]++;
        j++;
      }
    }

    return exponents;
  }

  /**
   * Builds the Matrix with all the exponents in the factorizations over
   * the smooth base of the factors to consider to find a square, by columns.
   *
   * The matrix has as many rows as elements in the factor base, and as
   * many columns as factors to consider for the square plus one. The more
   * factors we can consider the easier it will be to find a square.
   * The matrix has one more row to accomodate the independent term of the
   * linear system.
   *
   * @param inputArray is a {@link SieveArray}, with the factors to consider
   *     for finding squares.
   * @param factorBase is a set of integers to use for the decomposition.
   * @returns a {@link BitMatrix} with the augmented matrix of a linear system
   *     in base 2 whose solution can lead to finding a product of a subset
   *     of the factors that is a square.
   */
  public static BitMatrix buildSystemMatrix(SieveArray inputArray,
      FactorBaseArray factorBase) {
    // Initialize the system matrix.
    int rows = factorBase.size();
    int columns = inputArray.size() + 1;
    BitMatrix result = new BitMatrix(rows, columns);
    for (int i = 0; i < rows; i++) {
      for (int j = 0; j < columns; j++) {
        result.set(i, j, 0);
      }
    }

    // Decompose each factor in the array. The factors were taken because they
    // are smooth over the given factor base.
    for (int j = 0; j < columns - 1; j++) {
      int[] exponents = smoothFactor(inputArray.getEval(j), factorBase);

      // Copy to the System matrix, transposed.
      for (int i = 0; i < rows; i++) {
        int exponentMod2 = exponents[i] % 2;
        result.set(i, j, exponentMod2);
      }
    }

    LOGGER.info("System matrix of dimension: " + String.valueOf(rows)
        + " x " +  String.valueOf(columns));
    return result;
  }

  /**
   * Converts from an integer to a {@link BitMatrix} with its binary
   * representation. For each position the value will be 1 if the
   * corresponding bit in the integer is set or 0 if it's not set.
   *
   * The result will be used later to give values to the indeterminates
   * when solving the linear system.
   *
   * @param mask is an integer to use as bitmask.
   * @return A {@link BitMatrix} with the binary representation.
   */
  private static BitMatrix getIndeterminatesFromMask(int mask) {
    if (mask == 0) {
      BitMatrix result = new BitMatrix(1, 1);
      result.set(0, 0, 0);
      return result;
    }

    BigInteger binaryMask = BigInteger.valueOf(mask);
    int numIdeterminates = binaryMask.bitLength();
    BitMatrix indeterminates = new BitMatrix(numIdeterminates, 1);
    for (int i = 0; i < numIdeterminates; i++) {
      indeterminates.set(i, 0, binaryMask.testBit(i) ? 1 : 0);
    }

    return indeterminates;
  }

  /**
   * Attempts to find a subset of all the factors obtained in the sieve whose
   * product is a perfect square.
   *
   * There are possibly many subsets of products, which come from different
   * solutions of the linear system. The parameter {@code indeterminatesMask}
   * allows to obtain different solutions.
   *
   * If the result is not a valid subset (for a variety of reasons) it returns
   * null. Otherwise it returns a {@code SieveArray} with the factors.
   *
   * @param allFactors is a {@code SieveArray} with all the smooth factors
   *     obtained in the sieve phase.
   * @param indeterminatesMask is a bitmask used to choose different solutions
   *     of the linear system and different subsets of factors.
   * @param factorBase is the set of integers to use for the decomposition.
   * @returns {@code null} if it can't find a subset whose product is a square
   *     or a {@code SieveArray} with the factors if it can.
   * @throws {@link LinearSystem.EquationException} if it can't solve the
   *     system. This can't happen in a homogeneous system, so this exception
   *     would indicate that an error occured.
   */
  public static SieveArray findSquare(SieveArray allFactors,
       int indeterminatesMask, FactorBaseArray factorBase) throws
       BitMatrix.EquationException {
    // Build and solve the system. The result is a column vector.
    BitMatrix system = buildSystemMatrix(allFactors, factorBase);
    BitMatrix indeterminates = getIndeterminatesFromMask(indeterminatesMask);
    BitMatrix result = system.solve(indeterminates);

    // Get the factors in the square and their product.
    SieveArray squareFactors = SieveArray.newArray();
    BigInteger product = BigInteger.ONE;
    for (int i = 0; i < result.getRows(); i++) {
      // If the coeficient is 1 add the pair to the sieve array,
      // if it's 0, then we simply skip it.
      if (result.get(i, 0) == 1) {
        BigInteger integer = allFactors.getInt(i);
        BigInteger evaluation = allFactors.getEval(i);
        squareFactors.append(integer, evaluation);
        product = product.multiply(evaluation);
      }
    }

    // Check that the product is effectively a square.
    if (!BigIntegerMath.isSquare(product)) {
      LOGGER.severe("Find Squares found a set of factors whose product " +
          "is not a square. Factors: " + squareFactors.toString());
      return null;
    }

    return squareFactors;
  }

  /**
   * Tries to factor N.
   *
   * @param N is the integer to factor.
   * @param squareFactors is a subset of the sieved factors whose
   *     product is a square.
   * @return a {@code BigInteger} that is a factor of N or null if it
   *     can't find any factor.
   */
  public BigInteger tryFactor(BigInteger N, SieveArray squareFactors) {
    LOGGER.info("Attempting factorization with: " + squareFactors.toString());

    // Get the products of all the integers and their evaluations.
    BigInteger productInts = BigInteger.ONE;
    BigInteger productEvals = BigInteger.ONE;
    for (int i = 0; i < squareFactors.size(); i++) {
      productInts = productInts.multiply(squareFactors.getInt(i));
      productEvals = productEvals.multiply(squareFactors.getEval(i));
    }

    // Get the root of productEvals, we know it's a square.
    BigInteger productEvalsRoot = BigIntegerMath.sqrt(productEvals);

    // productEvals - productInts^2 is multiple of N.
    // Check if productEvalsRoot +- productInts share factors with N.
    // If we find a non trivial factor return it.
    BigInteger factor = N.gcd(productEvalsRoot.subtract(productInts));
    if (!factor.equals(BigInteger.ONE) && !factor.equals(N)) {
      return factor;
    }
    factor = N.gcd(productEvalsRoot.add(productInts));
    if (!factor.equals(BigInteger.ONE) && !factor.equals(N)) {
      return factor;
    }

    // Otherwise return null, we couldn't factor N.
    reducer_cant_factor.increment(1);
    return null;
  }

  /**
   * Setup prepares the tasks common for all calls to the reducer. It retrieves
   * the integer to factor and the factor base (set of primes to use for
   * smoothness) from the job context.
   *
   * @param context is the Reducer context.
   */
  protected void setup(Context context) throws IOException {
    // Initialize counters.
    reducer_invalid_sieve_array = context.getCounter("FindSquaresReducer",
        "invalid_sieve_array");
    reducer_unable_to_output = context.getCounter("FindSquaresReducer",
        "unable_to_output");
    reducer_unable_to_solve_system = context.getCounter("FindSquaresReducer",
        "unable_to_solve_system");
    reducer_cant_factor = context.getCounter("FindSquaresReducer",
        "cant_factor");

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
   * Perform the reduce phase of the MapReduce.
   *
   * @param key is the input key. It will be ignored.
   * @param values is a serialized {@link SieveArray} with the elements of the
   *     full sieve array that are smooth over the factor base.
   * @param context is the Reducer context, with common information for all
   *     the reducer jobs.
   */
  public void reduce(Text key, Iterable<Text> values, Context context) {
    // Add all the input values to a single SieveArray.
    SieveArray allFactors = null;
    Iterator<Text> valuesIterator = values.iterator();
    while (valuesIterator.hasNext()) {
      // Each input value is a SieveArray.
      String value = valuesIterator.next().toString();
      SieveArray sievedIntegers;
      try {
        sievedIntegers = SieveArray.fromString(value, 0);
      } catch (ParseException e) {
        reducer_invalid_sieve_array.increment(1);
        LOGGER.severe("Unable to read a Sieve Array input.");
        continue;
      }

      // Add to all factors.
      if (allFactors == null) {
        allFactors = sievedIntegers;
      } else {
        allFactors.append(sievedIntegers);
      }
    }

    // Try to find a subset of the factors that is a square.
    // Attempt with multiple values for the indeterminates in the solved system
    // Start at 1, because 0 usually gives the homogeneous solution.
    // With that subset, attempt the factorization of N.
    BigInteger factor = null;
    for (int attempt = 1; attempt < MAX_FIND_SQUARE_ATTEMPTS; attempt++) {
      LOGGER.info("Attempt: " + String.valueOf(attempt));

      SieveArray squareFactors = null;
      try {
        squareFactors = findSquare(allFactors, attempt, factorBase);
      } catch (BitMatrix.EquationException e) {
        reducer_unable_to_solve_system.increment(1);
        LOGGER.severe("An Equation Exception was thrown, this indicates an " +
                      "error in the resolution of the system.");
        continue;
      }
      if (squareFactors == null) {
        LOGGER.severe("Unable to get a solution for attempt: " +
            String.valueOf(attempt));
        continue;
      }

      // Attempt factorization. If it is successful we are done.
      factor = tryFactor(N, squareFactors);
      if (factor != null) {
        break;
      } else {
        LOGGER.info("The product didn't lead to a factorization of N");
      }
    }

    // If the factor is null it was impossible to factor N.
    if (factor == null) {
      LOGGER.severe("Unable to factor N after: " +
          String.valueOf(MAX_FIND_SQUARE_ATTEMPTS) + " attempts.");
      return;
    }

    // Otherwise output the factors.
    Text factor1Key = new Text("Factor1");
    Text factor2Key = new Text("Factor2");
    Text factor1Value = new Text(factor.toString());
    Text factor2Value = new Text(N.divide(factor).toString());
    try {
      context.write(factor1Key, factor1Value);
      context.write(factor2Key, factor2Value);
    } catch (IOException e) {
      reducer_unable_to_output.increment(1);
      LOGGER.severe("Unable to write the reduce output. IOException: " + e);
      return;
    } catch (InterruptedException e) {
      reducer_unable_to_output.increment(1);
      LOGGER.severe(
          "Unable to write the reduce output. InterrptedException: " + e);
      return;
    }
  }
}
