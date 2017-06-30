package com.linkedin.venice.unit.matchers;

import org.mockito.ArgumentMatcher;
import org.mockito.Matchers;

public class LongEqualOrGreaterThanMatcher extends ArgumentMatcher<Long> {
  private final long lowerBound;

  public LongEqualOrGreaterThanMatcher(long lowerBound) {
    this.lowerBound = lowerBound;
  }

  @Override
  public boolean matches(Object argument) {
    return argument instanceof Long && ((Long) argument) >= lowerBound;
  }

  public String toString() {
    return LongEqualOrGreaterThanMatcher.class.getSimpleName() + "(lowerBound=" + lowerBound + ")";
  }

  public static long get(long lowerBound) {
    return Matchers.longThat(new LongEqualOrGreaterThanMatcher(lowerBound));
  }
}
