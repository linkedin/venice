package com.linkedin.venice.logger;

import java.io.Serializable;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;


/**
 * A test log appender that can be used to verify log messages in unit tests.
 */
public class TestLogAppender extends AbstractAppender {
  // in-memory log to track log messages
  private StringBuilder log = new StringBuilder();

  public TestLogAppender(String name, Layout<? extends Serializable> layout) {
    super(name, null, layout, false, null);
  }

  @Override
  public void append(LogEvent event) {
    log.append(event.getMessage().getFormattedMessage()).append("\n");
  }

  /**
   * Use this api to verify log messages; once it's called, the log will be cleared.
   */
  public String getLog() {
    return log.toString();
  }
}
