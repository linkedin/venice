package com.linkedin.venice.stats.dimensions;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;


public class VeniceResponseStatusCategoryTest {
  @Test
  public void testVeniceResponseStatusCategory() {
    for (VeniceResponseStatusCategory responseStatusCategory: VeniceResponseStatusCategory.values()) {
      switch (responseStatusCategory) {
        case HEALTHY:
          assertEquals(responseStatusCategory.getCategory(), "healthy");
          break;
        case UNHEALTHY:
          assertEquals(responseStatusCategory.getCategory(), "unhealthy");
          break;
        case TARDY:
          assertEquals(responseStatusCategory.getCategory(), "tardy");
          break;
        case THROTTLED:
          assertEquals(responseStatusCategory.getCategory(), "throttled");
          break;
        case BAD_REQUEST:
          assertEquals(responseStatusCategory.getCategory(), "bad_request");
          break;
        default:
          throw new IllegalArgumentException("Unknown response status category: " + responseStatusCategory);
      }
    }
  }
}
