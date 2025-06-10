package com.linkedin.venice.client.store.predicate;

public class DoubleLowerThanPredicate implements DoublePredicate {
  private final double threshold;

  DoubleLowerThanPredicate(double threshold) {
    this.threshold = threshold;
  }

  @Override
  public boolean evaluate(double value) {
    return value < threshold;
  }
}
