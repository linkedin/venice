package com.linkedin.venice.unit.matchers;

import org.mockito.ArgumentMatcher;
import org.mockito.ArgumentMatchers;


public class LongEqualOrGreaterThanMatcher implements ArgumentMatcher<Long> {
  private final long lowerBound;

  public LongEqualOrGreaterThanMatcher(long lowerBound) {
    this.lowerBound = lowerBound;
  }

  @Override
  public boolean matches(Long argument) {
    return argument >= lowerBound;
  }

  public String toString() {
    return LongEqualOrGreaterThanMatcher.class.getSimpleName() + "(lowerBound=" + lowerBound + ")";
  }

  public static long get(long lowerBound) {
    return ArgumentMatchers.longThat(new LongEqualOrGreaterThanMatcher(lowerBound));
  }
}
