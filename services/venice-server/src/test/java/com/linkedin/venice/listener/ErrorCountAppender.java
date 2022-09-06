package com.linkedin.venice.listener;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;


public class ErrorCountAppender extends AbstractAppender {
  private final AtomicInteger errorMessageCounter;
  private final String exceptionMessage;

  protected ErrorCountAppender(
      String name,
      Filter filter,
      Layout<? extends Serializable> layout,
      boolean ignoreExceptions,
      Property[] properties,
      AtomicInteger errorMessageCounter,
      String exceptionMessage) {
    super(name, filter, layout, ignoreExceptions, properties);
    this.errorMessageCounter = errorMessageCounter;
    this.exceptionMessage = exceptionMessage;
  }

  public static class Builder<B extends Builder<B>> extends AbstractAppender.Builder<B>
      implements org.apache.logging.log4j.core.util.Builder<ErrorCountAppender> {
    private AtomicInteger errorMessageCounter;
    private String exceptionMessage;

    public B setErrorMessageCounter(AtomicInteger errorMessageCounter) {
      this.errorMessageCounter = errorMessageCounter;
      return asBuilder();
    }

    public B setExceptionMessage(String exceptionMessage) {
      this.exceptionMessage = exceptionMessage;
      return asBuilder();
    }

    @Override
    public ErrorCountAppender build() {
      return new ErrorCountAppender(
          "ErrorCountAppender",
          getFilter(),
          null,
          false,
          getPropertyArray(),
          errorMessageCounter,
          exceptionMessage);
    }
  }

  @Override
  public void append(LogEvent event) {
    if (event.getLevel().equals(Level.ERROR) && event.getThrown().getLocalizedMessage().contains(exceptionMessage)) {
      errorMessageCounter.addAndGet(1);
    }
  }
}
