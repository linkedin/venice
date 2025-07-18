package com.linkedin.venice.client.store.predicate;

public class IntLowerThanPredicate implements IntPredicate {
  private final int threshold;

  IntLowerThanPredicate(int threshold) {
    this.threshold = threshold;
  }

  @Override
  public boolean evaluate(int value) {
    return value < threshold;
  }

  @Override
  public String toString() {
    return "IntLowerThanPredicate{threshold=" + threshold + "}";
  }
}
