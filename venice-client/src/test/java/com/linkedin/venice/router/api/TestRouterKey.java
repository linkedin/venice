package com.linkedin.venice.router.api;

import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Created by mwise on 3/4/16.
 */
public class TestRouterKey {
  @Test
  public void encodesBase64(){
    String myKey = "myKey";
    RouterKey key = RouterKey.fromString(myKey);
    String b64Key = key.base64Encoded();
    Assert.assertEquals(b64Key, "bXlLZXk=", "key should b64 encode");
  }

  @Test
  public void testOrdering(){
    RouterKey a = new RouterKey("abc".getBytes());
    RouterKey b = new RouterKey("abcde".getBytes());
    RouterKey bb = new RouterKey("abcde".getBytes());
    RouterKey c = new RouterKey("b".getBytes());
    Assert.assertEquals(a.compareTo(b), -1, "RouterKey failed compareTo ordering");
    Assert.assertEquals(a.compareTo(c), -1, "RouterKey failed compareTo ordering");
    Assert.assertEquals(b.compareTo(a), 1, "RouterKey failed compareTo ordering");
    Assert.assertEquals(bb.compareTo(b), 0, "RouterKey failed compareTo ordering");
    Assert.assertEquals(a.compareTo(c), -1, "RouterKey failed compareTo ordering");
    Assert.assertEquals(c.compareTo(a), 1, "RouterKey failed compareTo ordering");
  }
}
