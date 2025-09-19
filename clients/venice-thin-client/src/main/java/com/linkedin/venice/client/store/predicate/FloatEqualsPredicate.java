package com.linkedin.venice.client.store.predicate;

public class FloatEqualsPredicate implements FloatPredicate {
  private final float expectedValue;
  private final float epsilon;

  FloatEqualsPredicate(float expectedValue, float epsilon) {
    this.expectedValue = expectedValue;
    this.epsilon = epsilon;
  }

  @Override
  public boolean evaluate(float value) {
    // Short-circuit exact equality first
    if (this.expectedValue == value) {
      return true;
    }
    // Handle NaN cases
    if (Float.isNaN(value)) {
      return Float.isNaN(expectedValue);
    }
    // Only check epsilon for finite numbers
    return Math.abs(value - expectedValue) <= epsilon;
  }

  @Override
  public String toString() {
    return "FloatEqualsPredicate{expectedValue=" + expectedValue + ", epsilon=" + epsilon + "}";
  }
}
