package com.linkedin.venice.unit.matchers;

import org.mockito.ArgumentMatcher;


public class ExceptionClassMatcher implements ArgumentMatcher<Exception> {
  private final Class exceptionClass;

  public ExceptionClassMatcher(Class exceptionClass) {
    this.exceptionClass = exceptionClass;
  }

  @Override
  public boolean matches(Exception argument) {
    return exceptionClass.isInstance(argument);
  }
}
