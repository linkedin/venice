package com.linkedin.venice.unit.matchers;

import org.mockito.ArgumentMatcher;

public class LongGreaterThanMatcher extends ArgumentMatcher<Long> {
  private final long lowerBound;

  public LongGreaterThanMatcher(long lowerBound) {
    this.lowerBound = lowerBound;
  }

  @Override
  public boolean matches(Object argument) {
    return argument instanceof Long && ((Long) argument) > lowerBound;
  }
}
