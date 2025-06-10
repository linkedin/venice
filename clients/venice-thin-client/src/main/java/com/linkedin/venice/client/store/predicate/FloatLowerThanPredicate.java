package com.linkedin.venice.client.store.predicate;

public class FloatLowerThanPredicate implements FloatPredicate {
  private final float threshold;

  FloatLowerThanPredicate(float threshold) {
    this.threshold = threshold;
  }

  @Override
  public boolean evaluate(float value) {
    return value < threshold;
  }
}
