package com.linkedin.venice.router.api;

import org.testng.Assert;
import org.testng.annotations.Test;


public class TestRouterKey {
  @Test
  public void encodesBase64() {
    String myKey = "myKey";
    RouterKey key = RouterKey.fromString(myKey);
    String b64Key = key.base64Encoded();
    Assert.assertEquals(b64Key, "bXlLZXk=", "key should b64 encode");
  }

  @Test
  public void testOrdering() {
    RouterKey a = new RouterKey("abc".getBytes());
    RouterKey b = new RouterKey("abcde".getBytes());
    RouterKey bb = new RouterKey("abcde".getBytes());
    RouterKey c = new RouterKey("b".getBytes());
    RouterKey d = new RouterKey("a".getBytes());
    doOrderingTest(a, b, -2);
    doOrderingTest(a, c, -1);
    doOrderingTest(b, a, 2);
    doOrderingTest(bb, b, 0);
    doOrderingTest(a, c, -1);
    doOrderingTest(c, a, 1);
    doOrderingTest(c, d, 1);
  }

  private String getKeyString(RouterKey key) {
    return new String(key.getKeyBuffer().array(), key.getKeyBuffer().position(), key.getKeyBuffer().remaining());
  }

  public void doOrderingTest(RouterKey left, RouterKey right, int expected) {
    Assert.assertEquals(
        left.compareTo(right),
        expected,
        "RouterKey failed to compare " + getKeyString(left) + " with " + getKeyString(right));
  }
}
