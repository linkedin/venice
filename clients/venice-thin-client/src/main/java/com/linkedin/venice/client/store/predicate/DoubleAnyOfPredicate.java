package com.linkedin.venice.client.store.predicate;

public class DoubleAnyOfPredicate implements DoublePredicate {
  private final double[] expectedValues;

  DoubleAnyOfPredicate(double... expectedValues) {
    this.expectedValues = expectedValues;
  }

  @Override
  public boolean evaluate(double value) {
    for (double expectedValue: expectedValues) {
      if (value == expectedValue) {
        return true;
      }
    }
    return false;
  }
}
