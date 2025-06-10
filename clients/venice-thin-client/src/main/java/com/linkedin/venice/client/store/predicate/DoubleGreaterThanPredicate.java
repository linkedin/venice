package com.linkedin.venice.client.store.predicate;

public class DoubleGreaterThanPredicate implements DoublePredicate {
  private final double threshold;

  DoubleGreaterThanPredicate(double threshold) {
    this.threshold = threshold;
  }

  @Override
  public boolean evaluate(double value) {
    return value > threshold;
  }
}
