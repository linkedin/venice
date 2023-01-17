package com.linkedin.venice.hadoop;

import org.testng.annotations.Test;


public class TestHelloWorldTestAgain {
  @Test
  public void testPositive() {
    HelloWorldTestAgain.checkNumber(1);
  }

  @Test
  public void testNegative() {
    HelloWorldTestAgain.checkNumber(-1);
  }
}
