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

import java.io.Serializable;
import java.lang.StringBuilder;
import java.math.BigInteger;
import java.text.ParseException;
import java.util.ArrayList;

/**
 * The FactorBaseArray is a utility class that holds a reference to the array
 * that contains the primes of the factor base.
 *
 * @see SieveArray.
 */
public class FactorBaseArray implements Serializable {

  /**
   * Array that contains the integers in the factor base.
   */
  protected ArrayList<BigInteger> factors;

  /**
   * Private constructor. In order to create a factor base array please use
   * {@code FactorBaseArray.fromArrayList} or
   * {@code FactorBaseArray.fromString}.
   */
  private FactorBaseArray() {}

  /**
   * Static constructor. It creates a new Factor Base Array and sets the array.
   *
   * @param integers The array of {@link bigInteger} in the sieve interval.
   * @return A new {@code FactorBaseArray} with the given array.
   */
  public static FactorBaseArray fromArrayList(ArrayList<BigInteger> factors) {
    FactorBaseArray s = new FactorBaseArray();
    s.factors = factors;
    return s;
  }

  /**
   * Static constructor. It creates a new Factor Base Array and sets the array.
   *
   * @param str a serialized form of a FactorBaseArray.
   * @return A new {@code FactorBaseArray} that is parsed from str.
   */
  public static FactorBaseArray fromString(String str)
      throws ParseException {
    ArrayList<BigInteger> factors = new ArrayList<BigInteger>();

    // Take out the opening and closing brackets.
    if (!str.startsWith("[") || !str.endsWith("]")) {
      throw new ParseException("Missing opening or closing brackets.", -1);
    }
    String tmpStr = str.substring(1, str.length() - 1);

    // Split into tokens, each one is a BigInteger.
    String[] ints = tmpStr.split(",");
    for (int i = 0; i < ints.length; i++) {
      factors.add(new BigInteger(ints[i]));
    }

    FactorBaseArray s = new FactorBaseArray();
    s.factors = factors;
    return s;
  }

  /**
   * Returns the size of the Factor Base Array.
   */
  public int size() {
    return factors.size();
  }

  /**
   * Returns the element of index i.
   */
  public BigInteger get(int i) {
    return factors.get(i);
  }

  /**
   * Returns the seralized form of the array. The format is as follows:
   * Factors:    2 3 5 7 17
   * Serialized: [2,3,5,7,17]
   *
   * @return A serialized form of the FactorBaseArray.
   */
  public String toString() {
    StringBuilder result = new StringBuilder();
    result.append("[");

    for (int i = 0; i < size(); i++) {
      result.append(get(i));

      if (i < size() - 1) {
        result.append(",");
      }
    }

    result.append("]");
    return result.toString();
  }
}
