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
    // Short-circuit exact equality first
    if (this.expectedValue == value) {
      return true;
    }
    // Handle NaN case
    if (Double.isNaN(value)) {
      return Double.isNaN(expectedValue);
    }
    // Only check epsilon for finite numbers
    return Math.abs(value - expectedValue) <= epsilon;
  }

  @Override
  public String toString() {
    return "DoubleEqualsPredicate{expectedValue=" + expectedValue + ", epsilon=" + epsilon + "}";
  }
}
