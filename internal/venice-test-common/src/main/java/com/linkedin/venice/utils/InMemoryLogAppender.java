package com.linkedin.venice.utils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;


public class InMemoryLogAppender extends AbstractAppender {
  private final List<String> logs = new ArrayList<>(128);

  protected InMemoryLogAppender(
      String name,
      Filter filter,
      Layout<? extends Serializable> layout,
      boolean ignoreExceptions,
      Property[] properties) {
    super(name, filter, layout, ignoreExceptions, properties);
  }

  public static class Builder extends AbstractAppender.Builder
      implements org.apache.logging.log4j.core.util.Builder<InMemoryLogAppender> {
    @Override
    public InMemoryLogAppender build() {
      return new InMemoryLogAppender(this.getClass().getSimpleName(), getFilter(), null, false, getPropertyArray());
    }
  }

  @Override
  public void append(LogEvent event) {
    logs.add(event.getMessage().getFormattedMessage());
  }

  public List<String> getLogs() {
    return logs;
  }
}
