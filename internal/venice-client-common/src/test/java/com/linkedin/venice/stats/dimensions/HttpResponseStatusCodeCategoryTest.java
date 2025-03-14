package com.linkedin.venice.stats.dimensions;

import static com.linkedin.venice.stats.dimensions.HttpResponseStatusCodeCategory.CLIENT_ERROR;
import static com.linkedin.venice.stats.dimensions.HttpResponseStatusCodeCategory.INFORMATIONAL;
import static com.linkedin.venice.stats.dimensions.HttpResponseStatusCodeCategory.REDIRECTION;
import static com.linkedin.venice.stats.dimensions.HttpResponseStatusCodeCategory.SERVER_ERROR;
import static com.linkedin.venice.stats.dimensions.HttpResponseStatusCodeCategory.SUCCESS;
import static com.linkedin.venice.stats.dimensions.HttpResponseStatusCodeCategory.UNKNOWN;
import static com.linkedin.venice.stats.dimensions.HttpResponseStatusCodeCategory.getVeniceHttpResponseStatusCodeCategory;
import static org.testng.Assert.assertEquals;

import com.linkedin.venice.utils.CollectionUtils;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.util.Map;
import org.testng.annotations.Test;


public class HttpResponseStatusCodeCategoryTest extends VeniceDimensionInterfaceTest<HttpResponseStatusCodeCategory> {
  protected HttpResponseStatusCodeCategoryTest() {
    super(HttpResponseStatusCodeCategory.class);
  }

  @Override
  protected VeniceMetricsDimensions expectedDimensionName() {
    return VeniceMetricsDimensions.HTTP_RESPONSE_STATUS_CODE_CATEGORY;
  }

  @Override
  protected Map<HttpResponseStatusCodeCategory, String> expectedDimensionValueMapping() {
    return CollectionUtils.<HttpResponseStatusCodeCategory, String>mapBuilder()
        .put(INFORMATIONAL, "1xx")
        .put(SUCCESS, "2xx")
        .put(REDIRECTION, "3xx")
        .put(CLIENT_ERROR, "4xx")
        .put(SERVER_ERROR, "5xx")
        .put(UNKNOWN, "unknown")
        .build();
  }

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
