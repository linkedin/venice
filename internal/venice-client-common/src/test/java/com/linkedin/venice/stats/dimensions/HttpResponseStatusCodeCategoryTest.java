package com.linkedin.venice.stats.dimensions;

import static com.linkedin.venice.stats.dimensions.HttpResponseStatusCodeCategory.CLIENT_ERROR;
import static com.linkedin.venice.stats.dimensions.HttpResponseStatusCodeCategory.INFORMATIONAL;
import static com.linkedin.venice.stats.dimensions.HttpResponseStatusCodeCategory.REDIRECTION;
import static com.linkedin.venice.stats.dimensions.HttpResponseStatusCodeCategory.SERVER_ERROR;
import static com.linkedin.venice.stats.dimensions.HttpResponseStatusCodeCategory.SUCCESS;
import static com.linkedin.venice.stats.dimensions.HttpResponseStatusCodeCategory.UNKNOWN;
import static com.linkedin.venice.stats.dimensions.HttpResponseStatusCodeCategory.getVeniceHttpResponseStatusCodeCategory;
import static org.testng.Assert.assertEquals;

import io.netty.handler.codec.http.HttpResponseStatus;
import org.testng.annotations.Test;


public class HttpResponseStatusCodeCategoryTest {
  @Test()
  public void testValues() {
    assertEquals(getVeniceHttpResponseStatusCodeCategory(HttpResponseStatus.PROCESSING), INFORMATIONAL);
    assertEquals(getVeniceHttpResponseStatusCodeCategory(HttpResponseStatus.OK), SUCCESS);
    assertEquals(getVeniceHttpResponseStatusCodeCategory(HttpResponseStatus.MOVED_PERMANENTLY), REDIRECTION);
    assertEquals(getVeniceHttpResponseStatusCodeCategory(HttpResponseStatus.BAD_REQUEST), CLIENT_ERROR);
    assertEquals(getVeniceHttpResponseStatusCodeCategory(HttpResponseStatus.INTERNAL_SERVER_ERROR), SERVER_ERROR);
  }

  @Test
  public void testUnknownCategory() {
    assertEquals(getVeniceHttpResponseStatusCodeCategory(HttpResponseStatus.valueOf(99)), UNKNOWN);
    assertEquals(getVeniceHttpResponseStatusCodeCategory(HttpResponseStatus.valueOf(600)), UNKNOWN);
  }
}
