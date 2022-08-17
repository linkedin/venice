package com.linkedin.venice.unit.matchers;

import org.mockito.ArgumentMatcher;


public class NonEmptyStringMatcher implements ArgumentMatcher<String> {
  @Override
  public boolean matches(String argument) {
    return !argument.isEmpty();
  }
}
