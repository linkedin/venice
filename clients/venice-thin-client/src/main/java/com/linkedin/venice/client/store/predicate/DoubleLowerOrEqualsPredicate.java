package com.linkedin.venice.client.store.predicate;

public class DoubleLowerOrEqualsPredicate implements DoublePredicate {
  private final double threshold;
  private final double epsilon;

  private static final double DEFAULT_EPSILON = 1e-10;

  DoubleLowerOrEqualsPredicate(double threshold) {
    this(threshold, DEFAULT_EPSILON);
  }

  DoubleLowerOrEqualsPredicate(double threshold, double epsilon) {
    this.threshold = threshold;
    this.epsilon = epsilon;
  }

  @Override
  public boolean evaluate(double value) {
    return (threshold - value) > -epsilon;
  }

  @Override
  public String toString() {
    return "DoubleLowerOrEqualsPredicate{threshold=" + threshold + ", epsilon=" + epsilon + "}";
  }
}
