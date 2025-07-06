package com.linkedin.venice.client.store.predicate;

public class IntEqualsPredicate implements IntPredicate {
  private final int expectedValue;

  IntEqualsPredicate(int expectedValue) {
    this.expectedValue = expectedValue;
  }

  @Override
  public boolean evaluate(int value) {
    return value == expectedValue;
  }

  @Override
  public String toString() {
    return "IntEqualsPredicate{expectedValue=" + expectedValue + "}";
  }
}
