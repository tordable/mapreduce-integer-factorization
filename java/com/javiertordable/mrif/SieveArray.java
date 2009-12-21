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
import java.math.BigInteger;
import java.lang.IllegalArgumentException;
import java.lang.StringBuilder;
import java.text.ParseException;
import java.util.ArrayList;

/**
 * The SieveArray is a utility class that holds references to the arrays
 * that contain the numbers to sieve and the evaluations of the fundamental
 * polynomial in each of them.
 *
 * @see FactorBaseArray.
 */
public class SieveArray implements Serializable {

  /**
   * Array that contains the integers in the sieve interval.
   */
  protected ArrayList<BigInteger> integers;

  /**
   * Array that contains the evaluations of the fundamental polynomial in each
   * of the integers above.
   */
  protected ArrayList<BigInteger> evaluations;

  /**
   * Private constructor. In order to create a sieve array please use
   * {@code SieveArray.fromArrayList}, {@code SieveArray.fromString} or
   * {@code SieveArray.newArray}.
   */
  private SieveArray() {}

  /**
   * Static constructor. Creates a new empty array.
   */
  public static SieveArray newArray() {
    SieveArray s = new SieveArray();
    s.integers = new ArrayList<BigInteger>();
    s.evaluations = new ArrayList<BigInteger>();
    return s;
  }

  /**
   * Static constructor. It creates a new Sieve Array and sets the arrays.
   *
   * @param integers The array of {@link BigInteger} in the sieve interval.
   * @param evaluations The evaluations of the previous integers through the
   *     fundamental polynomial.
   * @return A new {@code SieveArray} with the given arrays.
   * @throws {@code IllegalArgumentException} if both arrays don't have the
   *     same dimension.
   */
  public static SieveArray fromArrayLists(ArrayList<BigInteger> integers,
      ArrayList<BigInteger> evaluations) throws IllegalArgumentException {
    if (integers.size() != evaluations.size()) {
      throw new IllegalArgumentException("Arrays must have the same size");
    }

    SieveArray s = new SieveArray();
    s.integers = integers;
    s.integers.trimToSize();
    s.evaluations = evaluations;
    s.evaluations.trimToSize();
    return s;
  }

  /**
   * Static constructor. It creates a new Sieve Array and sets the arrays.
   *
   * @param str a serialized form of a SieveArray.
   * @param expectedSize is the expected size of the sieve interval or 0,
   *     in that case a default size will be assumed.
   * @return A new {@code SieveArray} that is parsed from str.
   */
  public static SieveArray fromString(String str, int expectedSize)
      throws ParseException {
    // Check if its an empty array. In that case return an empty SieveArray.
    if (str.equals("[]")) {
      SieveArray s = new SieveArray();
      s.integers = new ArrayList<BigInteger>();
      s.evaluations = new ArrayList<BigInteger>();
      return s;
    }

    // Take out the opening and closing brackets.
    if (!str.startsWith("[[") || !str.endsWith("]]")) {
      throw new ParseException("Missing opening or closing brackets.", -1);
    }
    String tmpStr = str.substring(2, str.length() - 2);

    // Initialize the storage arrays.
    int startSize = expectedSize > 0 ? expectedSize : 10;
    ArrayList<BigInteger> integers = new ArrayList<BigInteger>(startSize);
    ArrayList<BigInteger> evaluations = new ArrayList<BigInteger>(startSize);

    // Split into a,b tokens by removing all the ],[ between pairs,
    // and parse each big integer in the pair.
    String[] pairs = tmpStr.split("\\],\\[");
    for (int i = 0; i < pairs.length; i++) {
      String pair = pairs[i];
      String[] pairInts = pair.split(",");
      if (pairInts.length != 2) {
        throw new ParseException("All pairs must have only two integers.", -1);
      }

      integers.add(new BigInteger(pairInts[0]));
      evaluations.add(new BigInteger(pairInts[1]));
    }

    SieveArray s = new SieveArray();
    s.integers = integers;
    s.integers.trimToSize();
    s.evaluations = evaluations;
    s.evaluations.trimToSize();
    return s;
  }

  /**
   * Returns the size of the {@code SieveArray}.
   */
  public int size() {
    return integers.size();
  }

  /**
   * Returns the element of the array of index i.
   */
  public BigInteger getInt(int i) {
    return integers.get(i);
  }

  /**
   * Returns the evaluation of the fundamental polynomial in the element of
   * the sieve array of index i. Equivalently getEval(i) = p(getInt(i)).
   */
  public BigInteger getEval(int i) {
    return evaluations.get(i);
  }

  /**
   * Appends another {@code SieveArray} at the end of this one.
   *
   * @param other is a {@code SieveArray} to add at the end of this one.
   */
  public void append(SieveArray other) {
    integers.addAll(other.integers);
    integers.trimToSize();
    evaluations.addAll(other.evaluations);
    integers.trimToSize();
  }

  /**
   * Appends a pair of an integer and its evaluation.
   *
   * @param integer is an integer to append.
   * @param evaluation is the evaluation of the integer to append.
   */
  public void append(BigInteger integer, BigInteger evaluation) {
    integers.add(integer);
    evaluations.add(evaluation);
  }

  /**
   * Returns the seralized form of the array. The format is as follows:
   * Integers:    1 2 3 4
   * Evaluations: 5 6 7 8
   * Serialized:  [[1,5],[2,6],[3,7],[4,8]]
   *
   * @return A serialized form of the {@code SieveArray}.
   */
  public String toString() {
    StringBuilder result = new StringBuilder();
    result.append("[");

    for (int i = 0; i < size(); i++) {
      result.append("[");
      result.append(getInt(i));
      result.append(",");
      result.append(getEval(i));
      result.append("]");

      if (i < size() - 1) {
        result.append(",");
      }
    }

    result.append("]");
    return result.toString();
  }
}
