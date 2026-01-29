package com.linkedin.venice.client.store.predicate;

public class IntAnyOfPredicate implements IntPredicate {
  private final int[] expectedValues;

  IntAnyOfPredicate(int... expectedValues) {
    /**
     * TODO: We can also build a Set-based impl, and potentially decide which to pick based on the amount of values.
     *
     * But for the client-side, where we will potentially not even evaluate the predicate locally, just keeping a simple
     * array is fine (more compact, no processing needed).
     */
    this.expectedValues = expectedValues;
  }

  @Override
  public boolean evaluate(int value) {
    for (int expectedValue: expectedValues) {
      if (value == expectedValue) {
        return true;
      }
    }
    return false;
  }

  @Override
  public String toString() {
    return "IntAnyOfPredicate{expectedValues=" + java.util.Arrays.toString(expectedValues) + "}";
  }
}
