package com.linkedin.venice.router;

import com.linkedin.ddsstorage.router.api.ResourcePath;
import java.util.Base64;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Created by mwise on 3/4/16.
 */
public class TestVenicePathParser {

  @Test
  public void parsesQueries(){
    String uri = "read/store/key";
    VenicePathParser parser = new VenicePathParser(new NoopVersionFinder());
    Path path = parser.parseResourceUri(uri);
    String keyb64 = Base64.getEncoder().encodeToString("key".getBytes());
    Assert.assertEquals(path.getLocation(), "read/store_v0/" + keyb64 + "?" + VenicePathParser.B64FORMAT);

    Path path2 = parser.substitutePartitionKey(path, RouterKey.fromString("key2"));
    String key2b64 = Base64.getEncoder().encodeToString("key2".getBytes());
    Assert.assertEquals(path2.getLocation(), "read/store_v0/" + key2b64 + "?" + VenicePathParser.B64FORMAT);
  }

  @Test
  public void parsesB64Uri(){
    String myUri = "/read/storename/bXlLZXk=?f=b64";
    String expectedKey = "myKey";
    Path path = new VenicePathParser(new NoopVersionFinder()).parseResourceUri(myUri);
    Assert.assertEquals(path.getPartitionKey().getBytes(), expectedKey.getBytes(),
        new String(path.getPartitionKey().getBytes()) + " should match " + expectedKey);
  }

  @Test(expectedExceptions = VeniceRouterException.class)
  public void failsToParseOtherActions() {
    new VenicePathParser(new NoopVersionFinder()).parseResourceUri("/badaction/storename/key");
  }

  @Test
  public void validatesResourceNames() {
    String[] goodNames = {
        "goodName",
        "good_name_with_underscores",
        "good-name-with-dashes",
        "goodNameWithNumbers1234545"
    };

    for (String name : goodNames){
      Assert.assertTrue(VenicePathParser.isStoreNameValid(name), "Store name: " + name + " should be valid");
    }

    String[] badNames = {
        "bad name with space",
        "bad.name.with.dots",
        "8startswithnumber",
        "bad-name-that-is-just-fine-except-that-the-name-is-really-long-like-longer-than-128-chars-bad-name-that-is-just-fine-except-that-the-name-is-really-long-like-longer-than-128-chars"
    };

    for (String name : badNames){
      Assert.assertFalse(VenicePathParser.isStoreNameValid(name), "Store name: " + name + " should not be valid");
    }

  }
}
