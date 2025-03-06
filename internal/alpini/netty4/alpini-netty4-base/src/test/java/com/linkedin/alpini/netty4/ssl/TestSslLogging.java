package com.linkedin.alpini.netty4.ssl;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.annotations.Test;


public class TestSslLogging {
  @Test
  public void testLoggingNullThrowable() {
    // This test is to make sure that the code does not throw an exception when the Throwable is null
    final Logger LOG = LogManager.getLogger(SslInitializer.class);
    Throwable nullThrowable = null;
    LOG.log(Level.ERROR, "This is a ERROR level test message", nullThrowable);
    LOG.log(Level.INFO, "This is a INFO level test message", nullThrowable);
    LOG.log(Level.WARN, "This is a WARN level test message", nullThrowable);
  }
}
