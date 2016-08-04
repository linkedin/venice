package com.linkedin.venice.unit.matchers;

import org.mockito.ArgumentMatcher;

public class ExceptionClassMatcher extends ArgumentMatcher<Exception> {
  private final Class exceptionClass;

  public ExceptionClassMatcher(Class exceptionClass) {
    this.exceptionClass = exceptionClass;
  }

  @Override
  public boolean matches(Object argument) {
    return exceptionClass.isInstance(argument);
  }
}
