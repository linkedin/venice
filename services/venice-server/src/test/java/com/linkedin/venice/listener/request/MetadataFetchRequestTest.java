package com.linkedin.venice.listener.request;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.QueryAction;
import com.linkedin.venice.request.RequestHelper;
import java.net.URI;
import org.testng.Assert;
import org.testng.annotations.Test;


public class MetadataFetchRequestTest {
  @Test
  public void testParseGetValidHttpRequest() {
    String storeName = "test_store";
    String uri = "/" + QueryAction.METADATA.toString().toLowerCase() + "/" + storeName;
    MetadataFetchRequest testRequest =
        MetadataFetchRequest.parseGetHttpRequest(uri, RequestHelper.getRequestParts(URI.create(uri)));

    Assert.assertEquals(testRequest.getStoreName(), storeName);
  }

  @Test
  public void testParseGetInvalidHttpRequest() {
    String uri = "/" + QueryAction.METADATA.toString().toLowerCase();

    try {
      MetadataFetchRequest.parseGetHttpRequest(uri, RequestHelper.getRequestParts(URI.create(uri)));
      Assert.fail("Venice Exception was not thrown");
    } catch (VeniceException e) {
      Assert.assertEquals(e.getMessage(), "not a valid request for a METADATA action: " + uri);
    }
  }
}
