package com.linkedin.venice.router.api;

import com.linkedin.ddsstorage.netty4.misc.BasicFullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.Test;

import static com.linkedin.venice.router.api.VenicePathParser.*;
import static com.linkedin.venice.router.api.VenicePathParserHelper.*;
import static com.linkedin.venice.router.utils.VeniceRouterUtils.*;


/**
 * Created by mwise on 4/25/16.
 */
public class TestVenicePathParserHelper {
  @Test
  public void parsesResourceTypes(){
    String controllerUri = "http://myhost:1234/" + TYPE_MASTER_CONTROLLER;
    BasicFullHttpRequest request = new BasicFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET,
        controllerUri, -1, -1);

    Assert.assertEquals(parseRequest(request).getResourceType(), TYPE_MASTER_CONTROLLER);
    Assert.assertTrue(parseRequest(request).isInvalidStorageRequest());

    String storageUri = "http://myhost:1234/" + TYPE_STORAGE + "/storename/key?f=b64&fee=fi&foe=fum";
    request = new BasicFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET,
        storageUri, -1, -1);
    VenicePathParserHelper storageHelper = parseRequest(request);
    Assert.assertEquals(storageHelper.getResourceType(), TYPE_STORAGE);
    Assert.assertEquals(storageHelper.getResourceName(), "storename");
    Assert.assertEquals(storageHelper.getKey(), "key");
    Assert.assertFalse(storageHelper.isInvalidStorageRequest());

    Map<String, String> params = storageHelper.extractQueryParameters(request);
    Assert.assertTrue(params.containsKey("f"));
    Assert.assertTrue(params.containsValue("b64"));
    Assert.assertTrue(params.containsKey("fee"));
    Assert.assertTrue(params.containsValue("fi"));
    Assert.assertTrue(params.containsKey("foe"));
    Assert.assertTrue(params.containsValue("fum"));
    Assert.assertEquals(params.size(), 3);

    // verify the attr map content.
    VenicePathParserHelper helper = request.attr(PATHPARSER_ATTRIBUTE_KEY).get();
    Assert.assertEquals(helper, storageHelper);

    String otherUri = "http://myhost:1234/";
    request = new BasicFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET,
        otherUri, -1, -1);
    Assert.assertNull(parseRequest(request).getResourceType(),
        "Missing resource type should parse to null");
  }

  @Test
  public void testReusePathParserResult() {
    String storageUri = "http://myhost:1234/" + TYPE_STORAGE + "/storename/key?f=b64";
    BasicFullHttpRequest request = new BasicFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET,
        storageUri, -1, -1);
    VenicePathParserHelper helper = parseRequest(request);
    BasicFullHttpRequest dummyRequest =  new BasicFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "", -1, -1);

    dummyRequest.attr(PATHPARSER_ATTRIBUTE_KEY).set(helper);
    Assert.assertEquals(parseRequest(dummyRequest), helper);
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
    BasicFullHttpRequest request = new BasicFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET,
        keySchemaUri, -1, -1);
    VenicePathParserHelper helper = parseRequest(request);
    Assert.assertEquals(helper.getResourceType(), TYPE_KEY_SCHEMA);
    Assert.assertEquals(helper.getResourceName(), storeName);
    Assert.assertNull(helper.getKey());

    // Test null case for path
    Map<String, String> emptyParams = helper.extractQueryParameters(request);
    Assert.assertEquals(emptyParams.size(), 0);

    String valueSchemaUriForSingleSchema = host + TYPE_VALUE_SCHEMA + "/" + storeName + "/" + valueSchemaId;
    request = new BasicFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET,
        valueSchemaUriForSingleSchema, -1, -1);
    helper = parseRequest(request);
    Assert.assertEquals(helper.getResourceType(), TYPE_VALUE_SCHEMA);
    Assert.assertEquals(helper.getResourceName(), storeName);
    Assert.assertEquals(helper.getKey(), valueSchemaId);

    String valueSchemaUriForAllSchema = host + TYPE_VALUE_SCHEMA + "/" + storeName;
    request = new BasicFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET,
        valueSchemaUriForAllSchema, -1, -1);
    helper = parseRequest(request);
    Assert.assertEquals(helper.getResourceType(), TYPE_VALUE_SCHEMA);
    Assert.assertEquals(helper.getResourceName(), storeName);
    Assert.assertNull(helper.getKey());

    // Empty path
    String emptyUri = host;
    request = new BasicFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET,
        emptyUri, -1, -1);
    helper = parseRequest(request);
    Assert.assertNull(helper.getResourceType());
    Assert.assertNull(helper.getResourceName());
    Assert.assertNull(helper.getKey());

    // Path without resource name, but the extra slash
    String schemaWithResourceType = host + TYPE_KEY_SCHEMA + "/";
    request = new BasicFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET,
        schemaWithResourceType, -1, -1);
    helper = parseRequest(request);
    Assert.assertEquals(helper.getResourceType(), TYPE_KEY_SCHEMA);
    Assert.assertNull(helper.getResourceName());
    Assert.assertNull(helper.getKey());

    // Path without key, but the extra slash
    String schemaWithResourceName = host + TYPE_KEY_SCHEMA + "/" + storeName + "/";
    request = new BasicFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET,
        schemaWithResourceName, -1, -1);
    helper = parseRequest(request);
    Assert.assertEquals(helper.getResourceType(), TYPE_KEY_SCHEMA);
    Assert.assertEquals(helper.getResourceName(), storeName);
    Assert.assertNull(helper.getKey());
  }
}
