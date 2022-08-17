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
    Assert.assertFalse(ExceptionUtils.recursiveClassEquals(v, IOException.class));
  }
}
