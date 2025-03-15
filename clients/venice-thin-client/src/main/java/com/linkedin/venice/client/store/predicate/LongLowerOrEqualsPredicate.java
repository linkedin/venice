package com.linkedin.venice.client.store.predicate;

public class LongLowerOrEqualsPredicate implements LongPredicate {
  private final long threshold;

  LongLowerOrEqualsPredicate(long threshold) {
    this.threshold = threshold;
  }

  @Override
  public boolean evaluate(long value) {
    return value <= threshold;
  }
}
