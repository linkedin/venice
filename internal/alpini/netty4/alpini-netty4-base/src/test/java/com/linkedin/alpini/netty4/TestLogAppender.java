package com.linkedin.alpini.netty4;

import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.Log4J2LoggerFactory;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginElement;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.apache.logging.log4j.core.layout.PatternLayout;


/**
 * Based upon log appender from http://tanerdiler.blogspot.com/2015/07/log4j2-appender-for-junit-testing.html
 */
@Plugin(name = "TestLogAppender", category = "Core", elementType = "appender", printObject = true)
public class TestLogAppender extends AbstractAppender {
  private static final long serialVersionUID = -4319748955513985321L;

  static {
    InternalLoggerFactory.setDefaultFactory(Log4J2LoggerFactory.INSTANCE);
  }

  private final List<LogEvent> _logs = new ArrayList<>();

  protected TestLogAppender(
      String name,
      Filter filter,
      Layout<? extends Serializable> layout,
      final boolean ignoreExceptions) {
    super(name, filter, layout, ignoreExceptions);
  }

  // The append method is where the appender does the work.
  // Given a log event, you are free to do with it what you want.
  // This example demonstrates:
  // 1. Concurrency: this method may be called by multiple threads concurrently
  // 2. How to use layouts
  // 3. Error handling
  @Override
  public void append(LogEvent event) {
    _logs.add(event);
  }

  public void clearMessages() {
    _logs.clear();
  }

  public int size() {
    return _logs.size();
  }

  public String getMessage(int index) {
    if (_logs.isEmpty()) {
      return null;
    }
    return _logs.get(index).getMessage().getFormattedMessage();
  }

  // Your custom appender needs to declare a factory method
  // annotated with `@PluginFactory`. Log4j will parse the configuration
  // and call this factory method to construct an appender instance with
  // the configured attributes.
  @PluginFactory
  public static TestLogAppender createAppender(
      @PluginAttribute("name") String name,
      @PluginElement("Layout") Layout<? extends Serializable> layout,
      @PluginElement("Filter") final Filter filter,
      @PluginAttribute("otherAttribute") String otherAttribute) {
    if (name == null) {
      LOGGER.error("No name provided for TestLogAppender");
      return null;
    }
    if (layout == null) {
      layout = PatternLayout.createDefaultLayout();
    }
    return getInstance(name, layout, filter);
  }

  private static TestLogAppender getInstance(String name, Layout<? extends Serializable> layout, Filter filter) {
    return _instances.computeIfAbsent(name, n -> new TestLogAppender(n, filter, layout, true));
  }

  private static final ConcurrentMap<String, TestLogAppender> _instances = new ConcurrentHashMap<>();
}
