package com.linkedin.venice.router.api;

import org.testng.Assert;
import org.testng.annotations.Test;

import static com.linkedin.venice.router.api.VenicePathParser.*;


/**
 * Created by mwise on 4/25/16.
 */
public class TestVenicePathParserHelper {
  @Test
  public void parsesResourceTypes(){
    String controllerUri = "http://myhost:1234/" + TYPE_MASTER_CONTROLLER;
    Assert.assertEquals(new VenicePathParserHelper(controllerUri).getResourceType(), TYPE_MASTER_CONTROLLER);
    Assert.assertTrue(new VenicePathParserHelper(controllerUri).isInvalidStorageRequest());

    String storageUri = "http://myhost:1234/" + TYPE_STORAGE + "/storename/key?f=b64";
    VenicePathParserHelper storageHelper = new VenicePathParserHelper(storageUri);
    Assert.assertEquals(storageHelper.getResourceType(), TYPE_STORAGE);
    Assert.assertEquals(storageHelper.getResourceName(), "storename");
    Assert.assertEquals(storageHelper.getKey(), "key");
    Assert.assertFalse(storageHelper.isInvalidStorageRequest());

    String otherUri = "http://myhost:1234/";
    Assert.assertNull(new VenicePathParserHelper(otherUri).getResourceType(),
        "Missing resource type should parse to null");
  }

  @Test
  public void parseUriForSchemaRequest() {
    // Normal paths:
    // /key_schema/${storeName}
    // /value_schema/${storeName}/1
    // /value_schema/${storeName}
    String host = "http://myhost:1234/";
    String storeName = "test_store";
    String valueSchemaId = "1";
    String keySchemaUri = host + TYPE_KEY_SCHEMA + "/" + storeName;
    VenicePathParserHelper helper = new VenicePathParserHelper(keySchemaUri);
    Assert.assertEquals(helper.getResourceType(), TYPE_KEY_SCHEMA);
    Assert.assertEquals(helper.getResourceName(), storeName);
    Assert.assertNull(helper.getKey());

    String valueSchemaUriForSingleSchema = host + TYPE_VALUE_SCHEMA + "/" + storeName + "/" + valueSchemaId;
    helper = new VenicePathParserHelper(valueSchemaUriForSingleSchema);
    Assert.assertEquals(helper.getResourceType(), TYPE_VALUE_SCHEMA);
    Assert.assertEquals(helper.getResourceName(), storeName);
    Assert.assertEquals(helper.getKey(), valueSchemaId);

    String valueSchemaUriForAllSchema = host + TYPE_VALUE_SCHEMA + "/" + storeName;
    helper = new VenicePathParserHelper(valueSchemaUriForAllSchema);
    Assert.assertEquals(helper.getResourceType(), TYPE_VALUE_SCHEMA);
    Assert.assertEquals(helper.getResourceName(), storeName);
    Assert.assertNull(helper.getKey());

    // Empty path
    String emptyUri = host;
    helper = new VenicePathParserHelper(emptyUri);
    Assert.assertNull(helper.getResourceType());
    Assert.assertNull(helper.getResourceName());
    Assert.assertNull(helper.getKey());

    // Path without resource name, but the extra slash
    String schemaWithResourceType = host + TYPE_KEY_SCHEMA + "/";
    helper = new VenicePathParserHelper(schemaWithResourceType);
    Assert.assertEquals(helper.getResourceType(), TYPE_KEY_SCHEMA);
    Assert.assertNull(helper.getResourceName());
    Assert.assertNull(helper.getKey());

    // Path without key, but the extra slash
    String schemaWithResourceName = host + TYPE_KEY_SCHEMA + "/" + storeName + "/";
    helper = new VenicePathParserHelper(schemaWithResourceName);
    Assert.assertEquals(helper.getResourceType(), TYPE_KEY_SCHEMA);
    Assert.assertEquals(helper.getResourceName(), storeName);
    Assert.assertNull(helper.getKey());
  }
}
