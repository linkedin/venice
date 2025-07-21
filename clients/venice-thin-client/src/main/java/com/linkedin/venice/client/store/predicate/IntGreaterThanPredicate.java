package com.linkedin.venice.client.store.predicate;

public class IntGreaterThanPredicate implements IntPredicate {
  private final int threshold;

  IntGreaterThanPredicate(int threshold) {
    this.threshold = threshold;
  }

  @Override
  public boolean evaluate(int value) {
    return value > threshold;
  }

  @Override
  public String toString() {
    return "IntGreaterThanPredicate{threshold=" + threshold + "}";
  }
}
