package com.linkedin.venice.stats.dimensions;

import static com.linkedin.venice.stats.dimensions.HttpResponseStatusCodeCategory.getVeniceHttpResponseStatusCodeCategory;
import static org.testng.Assert.assertEquals;

import io.netty.handler.codec.http.HttpResponseStatus;
import org.testng.annotations.Test;


public class HttpResponseStatusCodeCategoryTest {
  @Test()
  public void testValues() {
    assertEquals(getVeniceHttpResponseStatusCodeCategory(HttpResponseStatus.PROCESSING), "1xx");
    assertEquals(getVeniceHttpResponseStatusCodeCategory(HttpResponseStatus.OK), "2xx");
    assertEquals(getVeniceHttpResponseStatusCodeCategory(HttpResponseStatus.MOVED_PERMANENTLY), "3xx");
    assertEquals(getVeniceHttpResponseStatusCodeCategory(HttpResponseStatus.BAD_REQUEST), "4xx");
    assertEquals(getVeniceHttpResponseStatusCodeCategory(HttpResponseStatus.INTERNAL_SERVER_ERROR), "5xx");
  }

  @Test
  public void testUnknownCategory() {
    assertEquals(getVeniceHttpResponseStatusCodeCategory(HttpResponseStatus.valueOf(99)), "unknown");
    assertEquals(getVeniceHttpResponseStatusCodeCategory(HttpResponseStatus.valueOf(600)), "unknown");
  }
}
