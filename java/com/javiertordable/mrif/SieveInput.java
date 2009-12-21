// Copyright 2009. Javier Tordable.
// Author: jt@javiertordable.com (Javier Tordable).

package com.javiertordable.mrif;

import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigInteger;
import java.lang.Math;
import java.lang.StringBuilder;
import java.util.ArrayList;

/**
 * This class contains utility methods for buiding the input of the Quadratic
 * Sieve Map Reduce. In particular methods for computing the factor base and
 * the sieve interval for a given integer.
 */
public class SieveInput {

  /**
   * Computes the optimal size of the factor base for the integer N.
   * The size is (exp(sqrt(ln(N).ln(ln(N)))^sqrt(2)/4. However in order to
   * approximate ln(N) in base e we will take log_2(N), which can be
   * approximated from the bit length of N and convert to ln(N), via:
   * ln(N) = log_2(N) / log_2(e) = log_2(N) * ln(2).
   *
   * @param N is the integer to factor.
   * @returns The optimal size of the factor base.
   */
  public static int factorBaseSize(BigInteger N) {
    int log2N = N.bitLength();
    double lnN = log2N * Math.log(2.0);
    double lnlnN = Math.log(lnN);
    double base = Math.exp(Math.sqrt(lnN * lnlnN));
    double exponent = Math.sqrt(2) / 4;
    double size = Math.pow(base, exponent);
    return (int) Math.ceil(size);
  }

  /**
   * Generates the factor base of the appropriate size for factoring N.
   * This factor base is composed of primes p such that (N/p) = 1.
   *
   * @param N is the integer to factor.
   * @returns The {@link FactorBaseArray} for factoring N.
   */
  public static FactorBaseArray factorBase(BigInteger N) {
    int size = factorBaseSize(N);

    ArrayList<BigInteger> a = new ArrayList<BigInteger>(size);
    int numPrimes = 0;
    BigInteger p = BigInteger.valueOf(2);
    do {
      if (BigIntegerMath.isPrimeByTrialDivision(p)
          && Legendre.symbol(N, p) == 1) {
        a.add(p);
        numPrimes++;
      }

      p = p.add(BigInteger.ONE);
    } while (numPrimes < size);

    return FactorBaseArray.fromArrayList(a);
  }

  /**
   * Gets the optimal size of the full sieve interval. The size is the cube of
   * the optimal size for the factor base.
   *
   * @param N is the integer to factor.
   * @returns The optimal size of the full sieve interval.
   */
  public static BigInteger fullSieveIntervalSize(BigInteger N) {
    return BigInteger.valueOf(factorBaseSize(N)).pow(3);
  }

  /**
   * Writes the full sieve interval.
   * Each line in the input file consists of a serialized {@link SieveArray},
   * and it will be processed as a single unit by the mapper.
   * The Hadoop MapReduce framework is free to distribute the lines
   * among the workers however it prefers. This is indeed what makes this
   * approach so strong and reliable against failures.
   *
   * @param N is the integer to factor.
   * @param fileName is the name of the input file.
   */
  public static void writeFullSieveInterval(BigInteger N, String fileName)
      throws IOException {
    FileWriter writer = new FileWriter(fileName);

    BigInteger size = fullSieveIntervalSize(N);

    // Center the numbers on sqrt(N), to minimize the values.
    // That is, the start is sqrt(N) - size/2
    BigInteger NRoot = BigIntegerMath.sqrt(N);
    BigInteger start = NRoot.subtract(size.divide(BigInteger.valueOf(2)));

    // Each subinterval will be stored in a SieveArray.
    int intervalSize = MapReduceQuadraticSieve.MAP_INPUT_INTERVAL_SIZE;
    ArrayList<BigInteger> ints = new ArrayList<BigInteger>(intervalSize);
    ArrayList<BigInteger> evals = new ArrayList<BigInteger>(intervalSize);

    // For all elements in the full sieve interval
    BigInteger numFactor = BigInteger.ZERO;
    int subIntervalFactor = 0;
    do {
      BigInteger factor = start.add(numFactor);
      BigInteger eval = factor.pow(2).subtract(N);  // x^2 - N
      ints.add(factor);
      evals.add(eval);      

      numFactor = numFactor.add(BigInteger.ONE);
      subIntervalFactor++;

      if (subIntervalFactor == intervalSize) {
        // If we reached the size of the subinterval, output it in the file,
        // create a new one and continue.
        SieveArray s = SieveArray.fromArrayLists(ints, evals);
        writer.write(s.toString() + "\n");
        ints.clear();
        evals.clear();
        subIntervalFactor = 0;
      } else if (numFactor.compareTo(size) == 0) {
        // If we reached the end of the whole interval, output the last split.
        SieveArray s = SieveArray.fromArrayLists(ints, evals);
        writer.write(s.toString() + "\n");
      }
    } while (numFactor.compareTo(size) < 0);

    writer.close();
  }
}
