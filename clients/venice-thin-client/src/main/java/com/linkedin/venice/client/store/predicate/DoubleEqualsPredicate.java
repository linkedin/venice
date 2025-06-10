package com.linkedin.venice.client.store.predicate;

public class DoubleEqualsPredicate implements DoublePredicate {
  private final double expectedValue;
  private final double epsilon;

  DoubleEqualsPredicate(double expectedValue, double epsilon) {
    this.expectedValue = expectedValue;
    this.epsilon = epsilon;
  }

  @Override
  public boolean evaluate(double value) {
    if (Double.isNaN(expectedValue) && Double.isNaN(value)) {
      return true;
    }
    if (Double.isInfinite(expectedValue) && Double.isInfinite(value)) {
      return expectedValue == value; // Compare positive/negative infinity
    }
    return !Double.isNaN(value) && !Double.isInfinite(value) && !Double.isNaN(expectedValue)
        && !Double.isInfinite(expectedValue) && Math.abs(value - expectedValue) <= epsilon;
  }
}
