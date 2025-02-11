package com.linkedin.venice.stats.dimensions;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;


public class VeniceResponseStatusCategoryTest {
  @Test
  public void testVeniceResponseStatusCategory() {
    for (VeniceResponseStatusCategory responseStatusCategory: VeniceResponseStatusCategory.values()) {
      switch (responseStatusCategory) {
        case SUCCESS:
          assertEquals(responseStatusCategory.getCategory(), "success");
          break;
        case FAIL:
          assertEquals(responseStatusCategory.getCategory(), "fail");
          break;
        default:
          throw new IllegalArgumentException("Unknown response status category: " + responseStatusCategory);
      }
    }
  }
}
