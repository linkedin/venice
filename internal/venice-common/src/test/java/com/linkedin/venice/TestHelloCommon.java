package com.linkedin.venice;

import org.testng.Assert;
import org.testng.annotations.Test;


public class TestHelloCommon {
  @Test
  public void testMethod() {
    String s = "Hello HelloCommon Job";
    Assert.assertTrue(HelloCommon.isValid(s));

    s = "";
    Assert.assertFalse(HelloCommon.isValid(s));

    s = "dsa";
    Assert.assertTrue(HelloCommon.isValid(s));
  }
}
