package com.linkedin.venice.unit.matchers;

import org.mockito.ArgumentMatcher;

public class NonEmptyStringMatcher extends ArgumentMatcher<String> {
  @Override
  public boolean matches(Object argument) {
    return argument instanceof String && !((String) argument).isEmpty();
  }
}
