package com.linkedin.venice.listener.request;

import com.linkedin.venice.request.RequestHelper;
import org.testng.Assert;
import org.testng.annotations.Test;


public class RequestHelperTest {
  @Test
  public void testGetRequestParts() {
    String storeName = "test_store";
    String action = "test_query";
    String uri = "/" + action + "/" + storeName;

    String[] requestParts = RequestHelper.getRequestParts(uri);

    Assert.assertEquals(requestParts[1], action);
    Assert.assertEquals(requestParts[2], storeName);
  }

  @Test
  public void testGetRequestPartsWithQuery() {
    String storeName = "test_store";
    String action = "test_query";
    String query = "key=value";
    String uri = "/" + action + "/" + storeName + "?" + query;

    String[] requestParts = RequestHelper.getRequestParts(uri);

    Assert.assertEquals(requestParts[1], action);
    Assert.assertEquals(requestParts[2], storeName + "?" + query);
  }
}
