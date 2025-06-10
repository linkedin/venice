package com.linkedin.venice.client.store.predicate;

public class DoubleLowerOrEqualsPredicate implements DoublePredicate {
  private final double threshold;

  DoubleLowerOrEqualsPredicate(double threshold) {
    this.threshold = threshold;
  }

  @Override
  public boolean evaluate(double value) {
    return value <= threshold;
  }
}
