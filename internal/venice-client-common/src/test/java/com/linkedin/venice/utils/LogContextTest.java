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

  @BeforeMethod
  public void setUp() {
    ThreadContext.clearAll();
  }

  @AfterMethod
  public void tearDown() {
    ThreadContext.clearAll();
  }

  @Test
  public void testBuilderCreatesCorrectContext() {
    LogContext context = LogContext.newBuilder().setRegionName(REGION_NAME).setComponentName(COMPONENT_NAME).build();

    assertNotNull(context);
    assertEquals(context.getRegionName(), REGION_NAME);
    assertEquals(context.getComponentName(), COMPONENT_NAME);
    assertEquals(context.toString(), COMPONENT_NAME + "|" + REGION_NAME);
  }

  @Test
  public void testUpdateThreadContextWithValidRegion() {
    LogContext.updateThreadContext(REGION_NAME);
    String value = ThreadContext.get(LogContext.LOG_CONTEXT_KEY);

    assertNotNull(value);
    assertEquals(value, REGION_NAME);
  }

  @Test
  public void testUpdateThreadContextWithNullRegion() {
    LogContext.updateThreadContext(null);
    assertNull(ThreadContext.get(LogContext.LOG_CONTEXT_KEY));
  }

  @Test
  public void testUpdateThreadContextWithBlankRegion() {
    LogContext.updateThreadContext("   ");
    assertNull(ThreadContext.get(LogContext.LOG_CONTEXT_KEY));
  }

  @Test
  public void testBuilderWithNullFieldsStillBuilds() {
    LogContext context = LogContext.newBuilder().build();

    assertNotNull(context);
    assertNull(context.getRegionName());
    assertNull(context.getComponentName());
    assertEquals(context.toString(), "null|null");
  }
}
