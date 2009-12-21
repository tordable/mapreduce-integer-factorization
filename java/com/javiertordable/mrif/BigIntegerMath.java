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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;

/**
 * This class contains utility methods to operate with {@link BigInteger}.
 * In particular a deterministic primality test and a method to compute
 * the approximate square root of a BigInteger.
 */
public class BigIntegerMath {

  // Default precision for BigDecimal.
  private static final int DEFAULT_SCALE = 10;

  // Default rounding mode for BigDecimal.
  private static final RoundingMode ROUNDING = RoundingMode.HALF_EVEN;

  /**
   * This is a quick implementation of a primality check. It will be used
   * with small primes so performance is not an issue here.
   *
   * For 10^60 the size of the factor base is ~ 10000 so the maximum number
   * that it will be necessary to test to get the factor base is ~ 100000.
   *
   * @param a is the {@link BigInteger} to check for primality.
   * @returns {@code true} if it is prime and false if not.
   */
  protected static boolean isPrimeByTrialDivision(BigInteger a) {
    for (BigInteger i = BigInteger.valueOf(2); i.compareTo(a) < 0;
        i = i.add(BigInteger.ONE)) {
      if (a.remainder(i).equals(BigInteger.ZERO)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Computes a first square root estimate, to initialize the Newton method.
   * This comes from: a = 2^(log2(a)) => sqrt(a) = a^1/2 = 2^(1/2 * log2(a)).
   *
   * @param a is the {@link BigInteger} to compute a square root estimate.
   * @return a {@link BigInteger} with an estimation of sqrt(a).
   */
  private static BigInteger squareRootEstimate(BigInteger a) {
    int log2aAprox = a.bitLength();
    int rootExp = log2aAprox / 2;
    return BigInteger.valueOf(2).pow(rootExp);
  }

  /**
   * Computes the square root of a BigInteger if it exists or returns
   * the biggest integer less than the root if not.
   *
   * In order to do this it uses Newton's algorithm, using the approximation
   * above. Once the adjustment in an iteration is less than 1, we return
   * the floor of the square root estimate.
   * The equation for the Newton method is: x_i+1 = x_i + (N - x_i^2) / (2 x_i).
   *
   * @param a is the {@link BigInteger} to compute the square root.
   * @returns The approximate square root of a.
   */
  public static BigInteger sqrt(BigInteger a) {
    // Check the input parameter
    if (a.compareTo(BigInteger.ZERO) < 0) {
      throw new IllegalArgumentException();
    }

    BigDecimal square = new BigDecimal(a);
    BigDecimal solution = new BigDecimal(squareRootEstimate(a));

    // Apply the Newton method to find the approximate solution.
    BigDecimal adjustment;
    do {
      adjustment = square.subtract(solution.pow(2));
      adjustment = adjustment.divide(solution, DEFAULT_SCALE, ROUNDING);
      adjustment = adjustment.divide(BigDecimal.valueOf(2), DEFAULT_SCALE,
          ROUNDING);
      solution = solution.add(adjustment);
    } while (Math.abs(adjustment.doubleValue()) > 1);

    // Return the lower integer approximation.
    return solution.toBigInteger();
  }

  /**
   * Checks if the parameter is a perfect square or not.
   *
   * @param a is the {@link BigInteger} to check for perfect square.
   * @returns {@code true} if the parameter is a perfect square.
   */
  public static boolean isSquare(BigInteger a) {
    if (a.compareTo(BigInteger.ZERO) < 0) {
      return false;
    }

    BigInteger root = sqrt(a);
    return root.multiply(root).equals(a);
  }
}
