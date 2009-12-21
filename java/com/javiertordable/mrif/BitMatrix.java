// Copyright 2009. Javier Tordable.
// Author: jt@javiertordable.com (Javier Tordable).

package com.javiertordable.mrif;

import java.lang.StringBuilder;
import java.text.ParseException;

/**
 * The BitMatrix is a matrix of bits. It stores them in a compact form in
 * memory. It basically reserves an array of bytes and for a given column
 * it calculates in which word it is. Using bit manipulation it gets a
 * particular value within the word.
 *
 * Note: For the quadratic sieve algorith, using any other kind of matrix
 * can bring serious problems. Independently of performance, an integer or
 * floating point matrix has rounding errors, even when the solution is
 * integer, or eve binary. These rounding errors can accumulate to create
 * an imprecise result. For problems not of a trivial size, it is essential
 * to solve the system in base 2.
 */
public class BitMatrix {

  // Matrix of integers where we store the data.
  private int[][] m;

  // Number of rows of the matrix.
  private final int rows;

  // Number of columns of the matrix.
  private final int columns;

  // Number of bits in an int variable.
  private static final int WORD_SIZE = 32;

  // Number of positions to shift to get a word index.
  private static final int WORD_SHIFT = 5;

  // Mask to & to get a displacement in a word.
  private static final int DISPLACEMENT_MASK = 31;

  /**
   * Exception class for errors in solving the linear system of equations.
   */
  public static class EquationException extends Exception {
    public EquationException(String msg) {
      super(msg);
    }
  }

  /**
   * Constructs a new BitMatrix with the given number of rows and columns.
   *
   * @param rows is the number of rows of the matrix.
   * @param columns is the number of columns in the matrix.
   */
  public BitMatrix(int rows, int columns) {
    if ((rows < 1) || (columns < 1)) {
      throw new IllegalArgumentException();
    }

    this.rows = rows;
    this.columns = columns;

    // Compute the necessary number of word columns.
    // It's the minimum number x such that 32 * x >= columns.
    int numWordsColumns = columns % WORD_SIZE == 0 ?
      columns / WORD_SIZE :
      columns / WORD_SIZE + 1;
    m = new int[rows][numWordsColumns];
  }

  /**
   * Gets the number of rows in the matrix.
   */
  public int getRows() {
    return rows;
  }

  /**
   * Gets the number of columns in the matrix.
   */
  public int getColumns() {
    return columns;
  }

  /**
   * Gets the value of an element in the matrix.
   */
  public int get(int row, int column) {
    int numWordColumn = column >> WORD_SHIFT;
    int word = m[row][numWordColumn];
    int displacementColumn = column & DISPLACEMENT_MASK;
    return (word >> displacementColumn) & 1;
  }

  /**
   * Sets the value of an element in the matrix.
   */
  public void set(int row, int column, int value) {
    int writeValue = value & 1;  // Value = 0 or 1
    int numWordColumn = column >> WORD_SHIFT;
    int word = m[row][numWordColumn];
    int displacementColumn = column & DISPLACEMENT_MASK;
    if (writeValue == 0) {
      int mask = ~(1 << displacementColumn);  // 1...101...1
      word = word & mask;
    } else {
      int mask = 1 << displacementColumn;
      word = word | mask;
    }
    m[row][numWordColumn] = word;
  }

  /**
   * Returns a new BitMatrix which is the transpose of this.
   */
  public BitMatrix transpose() {
    BitMatrix result = new BitMatrix(getColumns(), getRows());
    for (int i = 0; i < getRows(); i++) {
      for (int j = 0; j < getColumns(); j++) {
        result.set(j, i, get(i, j));
      }
    }
    return result;
  }

  /**
   * Exchanges two rows in the matrix, starting at {@code firstColumn}.
   *
   * @param firstRow is the first row to exchange.
   * @param secondRow is the second row to exchange.
   * @param firstColumn indicates on which column the exchange of elements
   *     will begin.
   */
  public void exchangeRows(int firstRow, int secondRow, int firstColumn) {
    int numWordColumn = firstColumn >> WORD_SHIFT;
    int displacementColumn = firstColumn & DISPLACEMENT_MASK;

    // The mask has 1 in the columns to switch.
    int mask = -1 << displacementColumn;

    // Exchange the bits in the first word.
    int firstWord = m[firstRow][numWordColumn];
    int secondWord = m[secondRow][numWordColumn];
    int firstTemp = firstWord & mask;
    int secondTemp = secondWord & mask;
    firstWord = (firstWord & ~mask) + secondTemp;
    secondWord = (secondWord & ~mask) + firstTemp;
    m[firstRow][numWordColumn] = firstWord;
    m[secondRow][numWordColumn] = secondWord;

    // Exchange the rest of the words.
    for (int i = numWordColumn + 1; i < m[0].length; i++) {
      int temp = m[firstRow][i];
      m[firstRow][i] = m[secondRow][i];
      m[secondRow][i] = temp;
    }
  }

  /**
   * Reduces a row using the given pivot row. This ammounts to adding the pivot
   * row mod 2 to the row to reduce, if and only if the element in the first
   * column to reduce is 1.
   *
   * @param pivotRow is the row which is used as pivot.
   * @param rowToReduce is the row which is modified.
   * @param firstColumn is the first column which will be modified. All
   *     columns before this one should be already 0 in both rows.
   */
  public void reduceRow(int pivotRow, int rowToReduce, int firstColumn) {
    int numWordColumn = firstColumn >> WORD_SHIFT;

    if (get(rowToReduce, firstColumn) == 1) {
      // Add the whole word. All previous bits should be 0.
      for (int i = numWordColumn; i < m[0].length; i++) {
        m[rowToReduce][i] = m[rowToReduce][i] ^ m[pivotRow][i];
      }
    }
  }

  /**
   * Reduces the system to superior triangular form.
   * This will never permutate the last column, which contains the independent
   * terms.
   *
   * @return The vector of permutations done in the columns, in order to
   *     get back the correct values of the variables.
   */
  private int[] reduceToSuperiorTriangular() {
    int rows = getRows();
    int columns = getColumns();

    // Initialize the array of permutations.
    int[] permutations = new int[columns - 1];
    for (int i = 0; i < permutations.length; i++) {
      permutations[i] = i;
    }

    int currentRow = 0;
    int currentColumn = 0;
    int maxColumn = columns - 1;  // Do not permute the last column.
    do {
      // Find a pivot element (which is 1).
      int pivotRow = currentRow;
      int pivotColumn = currentColumn;
      int pivot = 0;
      for (int i = currentRow; (i < rows) && (pivot == 0); i++) {
        for (int j = currentColumn; (j < maxColumn) && (pivot == 0); j++) {
          if (get(i, j) == 1) {
            pivot = 1;
            pivotRow = i;
            pivotColumn = j;
          }
        }
      }

      // If the pivot is 0, the rest of the system is 0 as well.
      if (pivot == 0) {
        break;
      }

      // Exchange the pivot row.
      if (pivotRow != currentRow) {
        exchangeRows(pivotRow, currentRow, currentColumn);
      }

      // Exchange the pivot column and update the variables positions.
      if (pivotColumn != currentColumn) {
        int temp;
        for (int k = 0; k < rows; k++) {
          temp = get(k, currentColumn);
          set(k, currentColumn, get(k, pivotColumn));
          set(k, pivotColumn, temp);
        }

        int tempIndex = permutations[currentColumn];
        permutations[currentColumn] = permutations[pivotColumn];
        permutations[pivotColumn] = tempIndex;
      }

      // Reduce all rows below the current row.
      for (int i = currentRow + 1; i < rows; i++) {
        reduceRow(currentRow, i, currentColumn);
      }

      currentRow++;
      currentColumn++;
    } while ((currentRow < rows) && (currentColumn < maxColumn));

    return permutations;
  }

  /**
   * Solves the linear system of equations modulo 2. It assigns the values in
   * indeterminates to the variables that are not determined in the system.
   * After getting the solution it applies the inverse of the permutation
   * to get the correct values for each variable.
   *
   * @param indetermintes is a {@link BitMatrix} with values to assign to the
   *     indeterminates. It is useful when solving the system multiple times
   *     with different inputs. It there are not enough values, it assigns
   *     the value 0 to the indeterminate. It should be a n x 1 matrix / vector.
   * @returns a {@link BitMatrix} with the solution of the system. The
   *     dimension is (columns - 1) x 1 (a column vector).
   */
  public BitMatrix solve(BitMatrix indeterminates) throws EquationException {
    if (indeterminates == null) {
      throw new IllegalArgumentException();
    }

    // Reduce to superior triangular and get the permutations done in the
    // columns of the matrix.
    int[] permutations = reduceToSuperiorTriangular();

    int rows = getRows();
    int columns = getColumns();

    // The range of the original matrix must be the same as the augmented one.
    int range = 0;
    for (int i = 0; (i < rows) && (i < columns); i++) {
      if (get(i, i) != 0) {
        range++;
      }
    }
    for (int i = range; i < rows; i++) {
      if (get(i, columns - 1) == 1) {
        throw new EquationException("The range of the expanded system" +
            "is greater than the range of the original system");
      }
    }

    // Create the result array. For all indeterminates (columns - range),
    // initialize them with the values in the parameter indeterminates.
    BitMatrix result = new BitMatrix(columns - 1, 1);
    for (int i = 0; i < range; i++) {
      result.set(i, 1, 0);
    }
    for (int i = range; i < result.getRows(); i++) {
      if (i - range < indeterminates.getRows()) {
        result.set(i, 0, indeterminates.get(i - range, 0));
      } else {
        result.set(i, 0, 0);
      }
    }

    // Solve the system, which is triangular superior.
    for (int i = range - 1; i >= 0; i--) {
      // Compute the factors to substract.
      int otherFactors = 0;
      for (int j = i + 1; j < columns - 1; j++) {
        otherFactors = otherFactors + (get(i, j) * result.get(j, 0));
      }
      otherFactors = otherFactors % 2;

      // Solve the equation ax + b = c => x = (c - b) / a.
      // Where c - b = c ^ b and a is always 1.
      result.set(i, 0, get(i, columns - 1) ^ otherFactors);
    }

    // Return the results inverting the permutation.
    for (int newPosition = 0; newPosition < columns - 1; newPosition++) {
      // Find the old position of the variable.
      int oldPosition = 0;
      for (int k = 0; k < permutations.length; k++) {
        if (permutations[k] == newPosition) {
          oldPosition = k;
          break;
        }
      }

      // Exchange result[variable] and result[position] if appropriate.
      // Also change the permutations now that they are fixed.
      if (newPosition != oldPosition) {
        int tempValue = result.get(oldPosition, 0);
        result.set(oldPosition, 0, result.get(newPosition, 0));
        result.set(newPosition, 0, tempValue);

        int tempIndex = permutations[oldPosition];
        permutations[oldPosition] = permutations[newPosition];
        permutations[newPosition] = tempIndex;
      }
    }

    return result;
  }

  /**
   * Returns a string representation of the {@code BitMatrix}.
   * Example, for a 3 x 4 matrix:
   * [0010]
   * [1100]
   * [0011]
   */
  public String toString() {
    StringBuilder builder = new StringBuilder();
    for (int i = 0; i < getRows(); i++) {
      builder.append("[");
      for (int j = 0; j < getColumns(); j++) {
        builder.append(get(i,j));
      }
      builder.append("]\n");
    }
    return builder.toString();
  }

  /**
   * Parses a String coming from a serialized {@code BitMatrix} and returns a
   * new {@code BitMatrix}.
   */
  public static BitMatrix fromString(String s) throws ParseException {
    // Split in rows.
    String[] splits = s.split("[\\[\\]]");
    if (splits.length < 1) {
      throw new ParseException("The number of rows is 0", -1);
    }

    // Get the number of rows, ignore empty splits.
    int rows = 0;
    String[] validSplits = new String[splits.length];
    for (int i = 0; i < splits.length; i++) {
      if (splits[i].length() > 1) {
	validSplits[rows] = splits[i];
        rows++;
      }
    }

    // Check the number of columns. Ignore empty splits.
    int columns = validSplits[0].length();
    for (int i = 0; i < rows; i++) {
      if ((validSplits[i].length() > 1) &&
          (validSplits[i].length() != columns)) {
        throw new ParseException("Row: " + String.valueOf(i) +
            " does not have the same length.", -1);
      }
    }

    BitMatrix result = new BitMatrix(rows, columns);
    for (int i = 0; i < rows; i++) {
      // Copy the values, if they are valid (0 or 1).
      for (int j = 0; j < columns; j++) {
        if (validSplits[i].charAt(j) == '0') {
          result.set(i, j, 0);
        } else if (validSplits[i].charAt(j) == '1') {
          result.set(i, j, 1);
	} else {
          throw new ParseException("Row: " + String.valueOf(i) +
              " column: " + String.valueOf(j) +
              " contains an invalid character (not 0 or 1).", -1);
        }
      }
    }

    return result;
  }
}
