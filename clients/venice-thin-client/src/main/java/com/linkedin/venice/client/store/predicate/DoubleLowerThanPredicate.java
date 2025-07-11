package com.linkedin.venice.client.store.predicate;

public class DoubleLowerThanPredicate implements DoublePredicate {
  private final double threshold;
  private final double epsilon;

  private static final double DEFAULT_EPSILON = 1e-10;

  DoubleLowerThanPredicate(double threshold) {
    this(threshold, DEFAULT_EPSILON);
  }

  DoubleLowerThanPredicate(double threshold, double epsilon) {
    this.threshold = threshold;
    this.epsilon = epsilon;
  }

  @Override
  public boolean evaluate(double value) {
    return (threshold - value) > epsilon;
  }

  @Override
  public String toString() {
    return "DoubleLowerThanPredicate{threshold=" + threshold + ", epsilon=" + epsilon + "}";
  }
}
