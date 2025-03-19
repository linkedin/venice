package com.linkedin.venice.client.store.predicate;

import java.util.Objects;


public class EqualsPredicate<T> implements Predicate<T> {
  private final T expectedValue;

  EqualsPredicate(T expectedValue) {
    this.expectedValue = expectedValue;
  }

  @Override
  public boolean evaluate(T value) {
    return Objects.equals(this.expectedValue, value);
  }
}
