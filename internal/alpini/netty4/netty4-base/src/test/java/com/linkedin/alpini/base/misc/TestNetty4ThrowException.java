package com.linkedin.alpini.base.misc;

import java.io.IOException;
import org.testng.annotations.Test;


public class TestNetty4ThrowException {
  @Test(groups = "unit", expectedExceptions = IOException.class, expectedExceptionsMessageRegExp = "Test Successful")
  public void simpleTest() {
    ExceptionUtil.throwException(new IOException("Test Successful"));
  }
}
