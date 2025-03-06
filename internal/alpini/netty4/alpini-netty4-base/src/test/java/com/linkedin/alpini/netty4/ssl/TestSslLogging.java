package com.linkedin.alpini.netty4.ssl;

import static org.testng.AssertJUnit.assertEquals;

import com.linkedin.alpini.netty4.TestLogAppender;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.Log4J2LoggerFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


@Test(groups = "unit")
public class TestSslLogging {
  private static TestLogAppender appender;
  private static Logger logger;

  @BeforeClass
  public static void setup() {
    InternalLoggerFactory.setDefaultFactory(Log4J2LoggerFactory.INSTANCE);

    // Obtain the root logger context and configuration
    LoggerContext context = (LoggerContext) LogManager.getContext(false);
    Configuration config = context.getConfiguration();

    // Create and add the custom appender
    appender = TestLogAppender.createAppender("TestLogAppender", null, null, null);
    appender.start();
    config.addAppender(appender);

    // Add the appender to the logger config
    LoggerConfig loggerConfig = config.getRootLogger();
    loggerConfig.addAppender(appender, null, null);
    context.updateLoggers(); // Make sure to update loggers to apply the new configuration

    // Get a logger instance
    logger = LogManager.getLogger(SslInitializer.class);
  }

  @AfterClass
  public static void teardown() {
    // Remove appender and stop it
    LoggerContext context = (LoggerContext) LogManager.getContext(false);
    Configuration config = context.getConfiguration();
    LoggerConfig loggerConfig = config.getRootLogger();
    loggerConfig.removeAppender(appender.getName());
    appender.stop();
  }

  @Test
  public void testLoggingNullThrowable() {
    // This test ensures that the code does not throw an exception when the Throwable is null
    Throwable nullThrowable = null;

    // Log the messages
    logger.log(org.apache.logging.log4j.Level.ERROR, "This is an ERROR level test message", nullThrowable);
    logger.log(org.apache.logging.log4j.Level.INFO, "This is an INFO level test message", nullThrowable);
    logger.log(org.apache.logging.log4j.Level.WARN, "This is a WARN level test message", nullThrowable);

    // Verify that messages were logged correctly
    assertEquals("Expected 3 log events, but found: " + appender.size(), 3, appender.size());
    assertEquals("This is an ERROR level test message", appender.getMessage(0));
    assertEquals("This is an INFO level test message", appender.getMessage(1));
    assertEquals("This is a WARN level test message", appender.getMessage(2));
  }
}
