package com.linkedin.venice.client.store.predicate;

public class LongLowerThanPredicate implements LongPredicate {
  private final long threshold;

  LongLowerThanPredicate(long threshold) {
    this.threshold = threshold;
  }

  @Override
  public boolean evaluate(long value) {
    return value < threshold;
  }

  @Override
  public String toString() {
    return "LongLowerThanPredicate{threshold=" + threshold + "}";
  }
}
