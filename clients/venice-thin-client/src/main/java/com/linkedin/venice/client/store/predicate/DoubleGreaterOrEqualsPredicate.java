package com.linkedin.venice.client.store.predicate;

public class DoubleGreaterOrEqualsPredicate implements DoublePredicate {
  private final double threshold;
  private final double epsilon;

  private static final double DEFAULT_EPSILON = 1e-10;

  DoubleGreaterOrEqualsPredicate(double threshold) {
    this(threshold, DEFAULT_EPSILON);
  }

  DoubleGreaterOrEqualsPredicate(double threshold, double epsilon) {
    this.threshold = threshold;
    this.epsilon = epsilon;
  }

  @Override
  public boolean evaluate(double value) {
    return (value - threshold) > -epsilon;
  }

  @Override
  public String toString() {
    return "DoubleGreaterOrEqualsPredicate{threshold=" + threshold + ", epsilon=" + epsilon + "}";
  }
}
