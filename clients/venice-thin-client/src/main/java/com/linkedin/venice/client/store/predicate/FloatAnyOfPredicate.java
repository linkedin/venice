package com.linkedin.venice.client.store.predicate;

public class FloatAnyOfPredicate implements FloatPredicate {
  private final float[] expectedValues;

  FloatAnyOfPredicate(float... expectedValues) {
    this.expectedValues = expectedValues;
  }

  @Override
  public boolean evaluate(float value) {
    for (float expectedValue: expectedValues) {
      if (value == expectedValue) {
        return true;
      }
    }
    return false;
  }
}
