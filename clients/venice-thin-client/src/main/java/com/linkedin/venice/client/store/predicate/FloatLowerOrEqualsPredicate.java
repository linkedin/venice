package com.linkedin.venice.client.store.predicate;

public class FloatLowerOrEqualsPredicate implements FloatPredicate {
  private final float threshold;

  FloatLowerOrEqualsPredicate(float threshold) {
    this.threshold = threshold;
  }

  @Override
  public boolean evaluate(float value) {
    return value <= threshold;
  }
}
