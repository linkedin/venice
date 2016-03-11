package com.linkedin.venice.listener;

import java.util.Base64;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Created by mwise on 3/10/16.
 */
public class GetRequestHttpHandlerTest {

  @Test
  public void parsesKeys(){
    String b64Key = "bWF0dCB3aXNlIGlzIGF3ZXNvbWU=";
    Base64.Decoder d = Base64.getDecoder();
    doTest("myKey", "myKey".getBytes());
    doTest("myKey?a=b", "myKey".getBytes());
    doTest("myKey?f=string", "myKey".getBytes());
    doTest("myKey?f=b65", "myKey".getBytes());
    doTest(b64Key + "?f=b64", d.decode(b64Key.getBytes()));
    doTest(b64Key + "?a=b&f=b64", d.decode(b64Key.getBytes()));
    doTest(b64Key + "?f=b64&a=b", d.decode(b64Key.getBytes()));
  }

  public void doTest(String urlString, byte[] expectedKey){
    byte[] parsedKey = GetRequestHttpHandler.getKeyBytesFromUrlKeyString(urlString);
    Assert.assertEquals(
        parsedKey,
        expectedKey,
        urlString + " not parsed correctly as key.  Parsed: " + new String(parsedKey));
  }

}
