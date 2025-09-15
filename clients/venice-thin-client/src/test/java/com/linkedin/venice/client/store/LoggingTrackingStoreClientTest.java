package com.linkedin.venice.client.store;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.utils.Utils;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class LoggingTrackingStoreClientTest {
  private LoggerContext ctx;
  private Configuration config;
  private LoggerConfig loggerConfig;
  private InMemoryAppender appender;

  @BeforeMethod
  void setUp() {
    ctx = (LoggerContext) LogManager.getContext(false);
    config = ctx.getConfiguration();

    appender = new InMemoryAppender("testAppender");
    appender.start();
    config.addAppender(appender);

    // Attach to the class’s logger config (important: attach to LoggerConfig, not Logger)
    String loggerName = LoggingTrackingStoreClient.class.getName();
    loggerConfig = config.getLoggerConfig(loggerName);
    loggerConfig.addAppender(appender, Level.DEBUG, null);
    ctx.updateLoggers();
  }

  @AfterMethod
  void tearDown() {
    loggerConfig.removeAppender("testAppender");
    appender.stop();
    ctx.updateLoggers();
  }

  @Test
  public void testLoggingWithException() {
    InternalAvroStoreClient<String, String> internalAvroStoreClient = mock(InternalAvroStoreClient.class);
    CompletableFuture<String> innerFuture = new CompletableFuture<>();
    doReturn(innerFuture).when(internalAvroStoreClient).get(anyString(), any(), anyLong());
    LoggingTrackingStoreClient<String, String> client = new LoggingTrackingStoreClient<>(internalAvroStoreClient);
    CompletableFuture future = client.get("key");
    innerFuture.completeExceptionally(new VeniceException("test"));
    Assert.assertTrue(appender.events.stream().anyMatch(logEvent -> logEvent.getLevel().equals(Level.WARN)));
    Assert.assertTrue(
        appender.events.stream()
            .anyMatch(logEvent -> logEvent.getMessage().getFormattedMessage().contains("with latency")));
    Assert.assertTrue(future.isCompletedExceptionally());
  }

  @Test
  public void testLoggingWithSlowRequest() {
    InternalAvroStoreClient<String, String> internalAvroStoreClient = mock(InternalAvroStoreClient.class);
    CompletableFuture<String> innerFuture = new CompletableFuture<>();
    doReturn(innerFuture).when(internalAvroStoreClient).get(anyString(), any(), anyLong());
    LoggingTrackingStoreClient<String, String> client = new LoggingTrackingStoreClient<>(internalAvroStoreClient);
    CompletableFuture future = client.get("key");
    Utils.sleep(100);
    innerFuture.complete(null);
    Assert.assertTrue(appender.events.stream().anyMatch(logEvent -> logEvent.getLevel().equals(Level.WARN)));
    Assert.assertTrue(
        appender.events.stream()
            .anyMatch(
                logEvent -> logEvent.getMessage()
                    .getFormattedMessage()
                    .contains("Receive high latency single get request for store")));
    Assert.assertTrue(future.isDone());
  }

  // Simple in-memory appender to capture events
  static class InMemoryAppender extends AbstractAppender {
    private final List<LogEvent> events = new CopyOnWriteArrayList<>();

    InMemoryAppender(String name) {
      super(name, null, PatternLayout.createDefaultLayout(), false, null);
    }

    @Override
    public void append(LogEvent event) {
      events.add(event.toImmutable());
    }

    List<LogEvent> events() {
      return events;
    }
  }
}
