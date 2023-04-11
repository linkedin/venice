package com.linkedin.venice;

import com.linkedin.venice.hadoop.HelloVPJ;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestHelloVPJ {
  @Test
  public void testMethod() {
    String s = "Hello Venice Push Job";
    Assert.assertTrue(HelloVPJ.isValid(s));

    s = "";
    Assert.assertFalse(HelloVPJ.isValid(s));
  }
}
