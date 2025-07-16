package com.linkedin.venice.client.store.predicate;

public class LongGreaterOrEqualsPredicate implements LongPredicate {
  private final long threshold;

  LongGreaterOrEqualsPredicate(long threshold) {
    this.threshold = threshold;
  }

  @Override
  public boolean evaluate(long value) {
    return value >= threshold;
  }

  @Override
  public String toString() {
    return "LongGreaterOrEqualsPredicate{threshold=" + threshold + "}";
  }
}
