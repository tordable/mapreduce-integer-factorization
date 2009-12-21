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

import java.math.BigInteger;

/**
 * The {@code Legendre} class implements essentially the computation of the
 * legendre symbol (a/p), for p odd prime. For now it is just a basic
 * computation, but it is possible to do multiple optimizations.
 */
public class Legendre {

  /**
   * Compute the Legendre symbol of (a/p).
   *
   * It is possible to do some optimizations in this computation, but for now
   * we just compute it using the Euler criterion.
   * (a/p) = a^((p-1)/2) (mod p)
   *
   * @param a an integer.
   * @param p a positive odd prime.
   * @throws ArithmeticException if the computed value is not valid.
   */
  public static int symbol(BigInteger a, BigInteger p) {
    if (a.remainder(p).equals(BigInteger.ZERO)) {
      return 0;
    }

    BigInteger exponent = p.subtract(BigInteger.ONE);
    exponent = exponent.divide(BigInteger.valueOf(2));
    BigInteger result = a.modPow(exponent, p); // 1 <= result <= p - 1

    if (result.equals(BigInteger.ONE)) {
      return 1;
    } else if (result.equals(p.subtract(BigInteger.ONE))) {
      return -1;
    } else {
      throw new ArithmeticException("Error computing the Legendre symbol.");
    }
  }
}
