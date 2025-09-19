package com.linkedin.venice.client.store.predicate;

public class IntGreaterOrEqualsPredicate implements IntPredicate {
  private final int threshold;

  IntGreaterOrEqualsPredicate(int threshold) {
    this.threshold = threshold;
  }

  @Override
  public boolean evaluate(int value) {
    return value >= threshold;
  }

  @Override
  public String toString() {
    return "IntGreaterOrEqualsPredicate{threshold=" + threshold + "}";
  }
}
