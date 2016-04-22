package com.linkedin.venice.router.api;

import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Created by mwise on 4/25/16.
 */
public class TestVenicePathParserHelper {
  @Test
  public void parsesResourceTypes(){
    String controllerUri = "http://myhost:1234/" + VenicePathParser.TYPE_CONTROLLER;
    Assert.assertEquals(new VenicePathParserHelper(controllerUri).getResourceType(), VenicePathParser.TYPE_CONTROLLER);
    Assert.assertTrue(new VenicePathParserHelper(controllerUri).isInvalidStorageRequest());

    String storageUri = "http://myhost:1234/" + VenicePathParser.TYPE_STORAGE + "/storename/key?f=b64";
    VenicePathParserHelper storageHelper = new VenicePathParserHelper(storageUri);
    Assert.assertEquals(storageHelper.getResourceType(), VenicePathParser.TYPE_STORAGE);
    Assert.assertEquals(storageHelper.getResourceName(), "storename");
    Assert.assertEquals(storageHelper.getKey(), "key");
    Assert.assertFalse(storageHelper.isInvalidStorageRequest());

    String otherUri = "http://myhost:1234/";
    Assert.assertNull(new VenicePathParserHelper(otherUri).getResourceType(),
        "Missing resource type should parse to null");

  }
}
