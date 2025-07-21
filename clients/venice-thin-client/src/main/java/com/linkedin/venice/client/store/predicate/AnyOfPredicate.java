package com.linkedin.venice.client.store.predicate;

import java.util.Objects;


public class AnyOfPredicate<T> implements Predicate<T> {
  private final T[] expectedValues;

  AnyOfPredicate(T... expectedValues) {
    /**
     * TODO: We can also build a Set-based impl, and potentially decide which to pick based on the amount of values.
     *
     * But for the client-side, where we will potentially not even evaluate the predicate locally, just keeping a simple
     * array is fine (more compact, no processing needed).
     */
    this.expectedValues = expectedValues;
  }

  @Override
  public boolean evaluate(T value) {
    for (T expectedValue: expectedValues) {
      if (Objects.equals(value, expectedValue)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public String toString() {
    return "AnyOfPredicate{expectedValues=" + java.util.Arrays.toString(expectedValues) + "}";
  }
}
