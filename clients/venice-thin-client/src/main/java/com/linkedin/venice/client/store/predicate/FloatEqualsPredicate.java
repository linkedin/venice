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
    if (Float.isNaN(expectedValue) && Float.isNaN(value)) {
      return true;
    }
    if (Float.isInfinite(expectedValue) && Float.isInfinite(value)) {
      return expectedValue == value; // Compare positive/negative infinity
    }
    return !Float.isNaN(value) && !Float.isInfinite(value) && !Float.isNaN(expectedValue)
        && !Float.isInfinite(expectedValue) && Math.abs(value - expectedValue) <= epsilon;
  }
}
