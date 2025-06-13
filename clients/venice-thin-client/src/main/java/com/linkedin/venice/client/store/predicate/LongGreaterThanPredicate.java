package com.linkedin.venice.client.store.predicate;

public class LongGreaterThanPredicate implements LongPredicate {
  private final long threshold;

  LongGreaterThanPredicate(long threshold) {
    this.threshold = threshold;
  }

  @Override
  public boolean evaluate(long value) {
    return value > threshold;
  }
}
