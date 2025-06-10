package com.linkedin.venice.client.store.predicate;

public class FloatGreaterThanPredicate implements FloatPredicate {
  private final float threshold;

  FloatGreaterThanPredicate(float threshold) {
    this.threshold = threshold;
  }

  @Override
  public boolean evaluate(float value) {
    return value > threshold;
  }
}
