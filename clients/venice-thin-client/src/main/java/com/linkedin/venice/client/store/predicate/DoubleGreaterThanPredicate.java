package com.linkedin.venice.client.store.predicate;

public class DoubleGreaterThanPredicate implements DoublePredicate {
  private final double threshold;
  private final double epsilon;

  private static final double DEFAULT_EPSILON = 1e-10;

  DoubleGreaterThanPredicate(double threshold) {
    this(threshold, DEFAULT_EPSILON);
  }

  DoubleGreaterThanPredicate(double threshold, double epsilon) {
    this.threshold = threshold;
    this.epsilon = epsilon;
  }

  @Override
  public boolean evaluate(double value) {
    return (value - threshold) > epsilon;
  }

  @Override
  public String toString() {
    return "DoubleGreaterThanPredicate{threshold=" + threshold + "}";
  }
}
