package com.linkedin.venice.unit.matchers;

import org.mockito.ArgumentMatcher;


public class ExceptionClassAndCauseClassMatcher implements ArgumentMatcher<Exception> {
  private final Class exceptionClass;
  private final Class causeClass;

  public ExceptionClassAndCauseClassMatcher(Class exceptionClass, Class causeClass) {
    this.exceptionClass = exceptionClass;
    this.causeClass = causeClass;
  }

  @Override
  public boolean matches(Exception argument) {
    return exceptionClass.isInstance(argument) && causeClass.isInstance(((Exception) argument).getCause());
  }
}
