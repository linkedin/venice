package com.linkedin.venice.utils;

import com.linkedin.venice.exceptions.VeniceException;
import java.io.IOException;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestExceptionUtils {
  @Test
  public void testRecursiveExceptionEquals() {
    Throwable io = new IOException("test io exception");
    Throwable v = new VeniceException("test venice exception");
    Throwable vio = new VeniceException("test venice exception wrapping io exception", io);
    Assert.assertTrue(ExceptionUtils.recursiveClassEquals(io, IOException.class));
    Assert.assertTrue(ExceptionUtils.recursiveClassEquals(vio, IOException.class));
    Assert.assertTrue(
        ExceptionUtils.recursiveClassEquals(vio, IOException.class, IllegalArgumentException.class),
        "If any of the classes match the exception, the function should return true.");
    Assert.assertFalse(ExceptionUtils.recursiveClassEquals(v, IOException.class));
    Assert.assertFalse(
        ExceptionUtils.recursiveClassEquals(v, IOException.class, IllegalArgumentException.class),
        "If none of the classes match the exception, the function should return false.");
  }

  @Test
  public void testRecursiveMessageContains() {
    Throwable io = new IOException("test io exception");
    Throwable v = new VeniceException("test venice exception");
    Throwable vio = new VeniceException("test venice exception wrapping io exception", io);
    Throwable vioWithNullMessage = new VeniceException(null, io);
    Assert.assertTrue(ExceptionUtils.recursiveMessageContains(io, "test io exception"));
    Assert.assertTrue(ExceptionUtils.recursiveMessageContains(vio, "test io exception"));
    Assert.assertTrue(ExceptionUtils.recursiveMessageContains(vioWithNullMessage, "test io exception"));
    Assert.assertFalse(ExceptionUtils.recursiveMessageContains(v, "test io exception"));
  }
}
