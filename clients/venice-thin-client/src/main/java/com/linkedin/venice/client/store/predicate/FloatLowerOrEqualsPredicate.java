package com.linkedin.venice.client.store.predicate;

public class FloatLowerOrEqualsPredicate implements FloatPredicate {
  private final float threshold;
  private final float epsilon;

  private static final float DEFAULT_EPSILON = 1e-6f;

  FloatLowerOrEqualsPredicate(float threshold) {
    this(threshold, DEFAULT_EPSILON);
  }

  FloatLowerOrEqualsPredicate(float threshold, float epsilon) {
    this.threshold = threshold;
    this.epsilon = epsilon;
  }

  @Override
  public boolean evaluate(float value) {
    return (threshold - value) > -epsilon;
  }

  @Override
  public String toString() {
    return "FloatLowerOrEqualsPredicate{threshold=" + threshold + ", epsilon=" + epsilon + "}";
  }
}
