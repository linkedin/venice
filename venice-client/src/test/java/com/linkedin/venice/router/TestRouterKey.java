package com.linkedin.venice.router;

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
}
