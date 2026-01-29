package com.linkedin.venice.client.store.predicate;

public class LongAnyOfPredicate implements LongPredicate {
  private final long[] expectedValues;

  LongAnyOfPredicate(long... expectedValues) {
    /**
     * TODO: We can also build a Set-based impl, and potentially decide which to pick based on the amount of values.
     *
     * But for the client-side, where we will potentially not even evaluate the predicate locally, just keeping a simple
     * array is fine (more compact, no processing needed).
     */
    this.expectedValues = expectedValues;
  }

  @Override
  public boolean evaluate(long value) {
    for (long expectedValue: expectedValues) {
      if (value == expectedValue) {
        return true;
      }
    }
    return false;
  }

  @Override
  public String toString() {
    return "LongAnyOfPredicate{expectedValues=" + java.util.Arrays.toString(expectedValues) + "}";
  }
}
