package com.linkedin.venice.client.store.predicate;

public class FloatAnyOfPredicate implements FloatPredicate {
  private final float[] expectedValues;
  private final float epsilon;

  private static final float DEFAULT_EPSILON = 1e-6f;

  FloatAnyOfPredicate(float... expectedValues) {
    this(expectedValues, DEFAULT_EPSILON);
  }

  FloatAnyOfPredicate(float[] expectedValues, float epsilon) {
    this.expectedValues = expectedValues;
    this.epsilon = epsilon;
  }

  @Override
  public boolean evaluate(float value) {
    for (float expectedValue: expectedValues) {
      // Short-circuit exact equality first
      if (value == expectedValue) {
        return true;
      }
      // Handle NaN cases
      if (Float.isNaN(value)) {
        return Float.isNaN(expectedValue);
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
    return "FloatAnyOfPredicate{expectedValues=" + java.util.Arrays.toString(expectedValues) + ", epsilon=" + epsilon
        + "}";
  }
}
