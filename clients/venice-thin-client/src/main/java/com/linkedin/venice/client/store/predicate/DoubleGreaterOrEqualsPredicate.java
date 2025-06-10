package com.linkedin.venice.client.store.predicate;

public class DoubleGreaterOrEqualsPredicate implements DoublePredicate {
  private final double threshold;

  DoubleGreaterOrEqualsPredicate(double threshold) {
    this.threshold = threshold;
  }

  @Override
  public boolean evaluate(double value) {
    return value >= threshold;
  }
}
