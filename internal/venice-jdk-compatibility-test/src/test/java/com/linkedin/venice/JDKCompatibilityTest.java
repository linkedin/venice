package com.linkedin.venice;

import java.nio.ByteBuffer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;


public class JDKCompatibilityTest {
  private static final Logger LOGGER = LogManager.getLogger(JDKCompatibilityTest.class);

  @Test
  public void testJava8Runtime() {
    String javaRuntime = System.getProperty("java.version");
    LOGGER.info("Java runtime: {}", javaRuntime);
    ByteBuffer buffer = ByteBuffer.wrap(new byte[] { 1, 2, 3 });
    try {
      buffer.position(1);
    } catch (Throwable e) {
      Assert.fail("Test failed with Java runtime: " + javaRuntime, e);
    }
    Assert.assertEquals(buffer.get(), 2);
  }
}
