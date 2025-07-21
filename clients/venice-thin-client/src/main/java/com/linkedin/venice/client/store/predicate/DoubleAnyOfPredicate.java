package com.linkedin.venice.client.store.predicate;

public class DoubleAnyOfPredicate implements DoublePredicate {
  private final double[] expectedValues;
  private final double epsilon;

  private static final double DEFAULT_EPSILON = 1e-10;

  DoubleAnyOfPredicate(double... expectedValues) {
    this(expectedValues, DEFAULT_EPSILON);
  }

  DoubleAnyOfPredicate(double[] expectedValues, double epsilon) {
    this.expectedValues = expectedValues;
    this.epsilon = epsilon;
  }

  @Override
  public boolean evaluate(double value) {
    for (double expectedValue: expectedValues) {
      // Short-circuit exact equality first
      if (value == expectedValue) {
        return true;
      }
      // Handle NaN cases
      if (Double.isNaN(value)) {
        return Double.isNaN(expectedValue);
      }
      // Only check epsilon for finite numbers
      if (Math.abs(value - expectedValue) <= epsilon) {
        return true;
      }
    }
    return false;
  }

  @Override
  public String toString() {
    return "DoubleAnyOfPredicate{expectedValues=" + java.util.Arrays.toString(expectedValues) + ", epsilon=" + epsilon
        + "}";
  }
}
