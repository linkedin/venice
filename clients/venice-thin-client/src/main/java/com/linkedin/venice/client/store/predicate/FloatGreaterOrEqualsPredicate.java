package com.linkedin.venice.client.store.predicate;

public class FloatGreaterOrEqualsPredicate implements FloatPredicate {
  private final float threshold;

  FloatGreaterOrEqualsPredicate(float threshold) {
    this.threshold = threshold;
  }

  @Override
  public boolean evaluate(float value) {
    return value >= threshold;
  }
}
