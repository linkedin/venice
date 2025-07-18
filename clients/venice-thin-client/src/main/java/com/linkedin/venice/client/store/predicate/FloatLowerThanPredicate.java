package com.linkedin.venice.client.store.predicate;

public class FloatLowerThanPredicate implements FloatPredicate {
  private final float threshold;
  private final float epsilon;

  private static final float DEFAULT_EPSILON = 1e-6f;

  FloatLowerThanPredicate(float threshold) {
    this(threshold, DEFAULT_EPSILON);
  }

  FloatLowerThanPredicate(float threshold, float epsilon) {
    this.threshold = threshold;
    this.epsilon = epsilon;
  }

  @Override
  public boolean evaluate(float value) {
    return (threshold - value) > epsilon;
  }

  @Override
  public String toString() {
    return "FloatLowerThanPredicate{threshold=" + threshold + ", epsilon=" + epsilon + "}";
  }
}
