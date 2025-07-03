package com.linkedin.venice.client.store.predicate;

public class FloatGreaterThanPredicate implements FloatPredicate {
  private final float threshold;
  private final float epsilon;

  private static final float DEFAULT_EPSILON = 1e-6f;

  FloatGreaterThanPredicate(float threshold) {
    this(threshold, DEFAULT_EPSILON);
  }

  FloatGreaterThanPredicate(float threshold, float epsilon) {
    this.threshold = threshold;
    this.epsilon = epsilon;
  }

  @Override
  public boolean evaluate(float value) {
    return (value - threshold) > epsilon;
  }

  @Override
  public String toString() {
    return "FloatGreaterThanPredicate{threshold=" + threshold + "}";
  }
}
