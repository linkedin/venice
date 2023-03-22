package com.linkedin.venice.listener.request;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.QueryAction;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpVersion;
import org.testng.Assert;
import org.testng.annotations.Test;


public class MetadataFetchRequestTest {
  @Test
  public void testParseGetValidHttpRequest() {
    String storeName = "test_store";
    String uri = "/" + QueryAction.METADATA.toString().toLowerCase() + "/" + storeName;
    HttpRequest httpRequest = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, uri);
    MetadataFetchRequest testRequest = MetadataFetchRequest.parseGetHttpRequest(httpRequest);

    Assert.assertEquals(testRequest.getStoreName(), storeName);
  }

  @Test
  public void testParseGetInvalidHttpRequest() {
    String uri = "/" + QueryAction.METADATA.toString().toLowerCase();
    HttpRequest httpRequest = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, uri);

    try {
      MetadataFetchRequest.parseGetHttpRequest(httpRequest);
      Assert.fail("Venice Exception was not thrown");
    } catch (VeniceException e) {
      Assert.assertEquals(e.getMessage(), "not a valid request for a METADATA action: " + uri);
    }
  }
}
