package com.linkedin.venice.client.store.predicate;

public class LongEqualsPredicate implements LongPredicate {
  private final long expectedValue;

  LongEqualsPredicate(long expectedValue) {
    this.expectedValue = expectedValue;
  }

  @Override
  public boolean evaluate(long value) {
    return value == expectedValue;
  }

  @Override
  public String toString() {
    return "LongEqualsPredicate{expectedValue=" + expectedValue + "}";
  }
}
