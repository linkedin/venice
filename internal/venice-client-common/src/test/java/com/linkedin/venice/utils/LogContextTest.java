package com.linkedin.venice.utils;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import org.apache.logging.log4j.ThreadContext;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class LogContextTest {
  private static final String REGION_NAME = "us-west-1";
  private static final String COMPONENT_NAME = "router";
  private static final String INSTANCE_ID = "instance_1235";

  @BeforeMethod
  public void setUp() {
    LogContext.clearLogContext();
  }

  @AfterMethod
  public void tearDown() {
    LogContext.clearLogContext();
  }

  @Test
  public void testBuilderCreatesCorrectContext() {
    LogContext context = LogContext.newBuilder()
        .setRegionName(REGION_NAME)
        .setInstanceName(INSTANCE_ID)
        .setComponentName(COMPONENT_NAME)
        .build();

    assertNotNull(context);
    assertEquals(context.getRegionName(), REGION_NAME);
    assertEquals(context.getComponentName(), COMPONENT_NAME);
    assertEquals(context.toString(), COMPONENT_NAME + ":" + REGION_NAME + ":" + INSTANCE_ID);
  }

  @Test
  public void testBuilderWithNullFieldsStillBuilds() {
    LogContext context = LogContext.newBuilder().build();

    assertNotNull(context);
    assertNull(context.getRegionName());
    assertNull(context.getComponentName());
    assertEquals(context.toString(), "");
  }

  @Test
  public void testSetLogContextWithValidContext() {
    LogContext logContext = LogContext.newBuilder().setRegionName(REGION_NAME).setComponentName(COMPONENT_NAME).build();

    LogContext.setLogContext(logContext);
    String actual = ThreadContext.get(LogContext.LOG_CONTEXT_KEY);

    assertNotNull(actual);
    assertEquals(actual, logContext.toString());
  }

  @Test
  public void testSetLogContextWithNullRegionAndComponent() {
    LogContext logContext = LogContext.newBuilder().build();

    LogContext.setLogContext(logContext);
    String actual = ThreadContext.get(LogContext.LOG_CONTEXT_KEY);

    // Expecting "null|null", but blank check should skip it
    assertNull(actual);
  }

  @Test
  public void testSetLogContextWithBlankContextString() {
    // Simulate blank output from getValue (e.g., empty strings)
    LogContext logContext = LogContext.newBuilder().setRegionName(" ").setComponentName(" ").build();

    LogContext.setLogContext(logContext);
    assertNull(ThreadContext.get(LogContext.LOG_CONTEXT_KEY));
  }

  @Test
  public void testSetLogContextWithNullLogContext() {
    // should not throw NPE
    LogContext.setLogContext(null);
  }

  @Test
  public void testSetLogContextOverwritesPreviousContext() {
    LogContext first = LogContext.newBuilder().setRegionName("us-east-1").setComponentName("controller").build();

    LogContext second = LogContext.newBuilder().setRegionName("us-west-2").setComponentName("router").build();

    LogContext.setLogContext(first);
    assertEquals(ThreadContext.get(LogContext.LOG_CONTEXT_KEY), first.toString());

    LogContext.setLogContext(second);
    assertEquals(ThreadContext.get(LogContext.LOG_CONTEXT_KEY), second.toString());
  }

  @Test
  public void testSetLogContextWithString() {
    String expected = "test-region";
    LogContext.setLogContext(expected);

    String actual = ThreadContext.get(LogContext.LOG_CONTEXT_KEY);
    assertNotNull(actual);
    assertEquals(actual, expected);
  }

  @Test
  public void testSetLogContextWithLogContextObject() {
    LogContext context = LogContext.newBuilder().setComponentName("server").setRegionName("us-east-1").build();

    LogContext.setLogContext(context);

    String actual = ThreadContext.get(LogContext.LOG_CONTEXT_KEY);
    assertNotNull(actual);
    assertEquals(actual, context.toString());
  }

  @Test
  public void testSetLogContextWithNull() {
    LogContext.setLogContext(null);
    String actual = ThreadContext.get(LogContext.LOG_CONTEXT_KEY);
    assertNull(actual);
  }

  @Test
  public void testSetLogContextWithUnsupportedType() {
    Object unsupported = new Object();
    LogContext.setLogContext(unsupported);

    String actual = ThreadContext.get(LogContext.LOG_CONTEXT_KEY);
    assertNull(actual); // Nothing should have been set
  }

  @Test
  public void testClearLogContextRemovesKey() {
    ThreadContext.put(LogContext.LOG_CONTEXT_KEY, "test-value");
    assertNotNull(ThreadContext.get(LogContext.LOG_CONTEXT_KEY));

    LogContext.clearLogContext();

    assertNull(ThreadContext.get(LogContext.LOG_CONTEXT_KEY));
  }

  @Test
  public void testClearLogContextDoesNotThrowWhenKeyAbsent() {
    assertNull(ThreadContext.get(LogContext.LOG_CONTEXT_KEY));

    // Should not throw any exception even if the key is not present
    LogContext.clearLogContext();

    // Still null, but no exception occurred
    assertNull(ThreadContext.get(LogContext.LOG_CONTEXT_KEY));
  }
}
