package com.linkedin.venice.stats.dimensions;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;

import org.testng.annotations.Test;


public class VeniceHttpResponseStatusCodeCategoryTest {
  @Test()
  public void testValues() {
    for (VeniceHttpResponseStatusCodeCategory category: VeniceHttpResponseStatusCodeCategory.values()) {
      switch (category) {
        case INFORMATIONAL:
          assertEquals(category.getCategory(), "1xx");
          assertEquals(category.getMin(), 100);
          assertEquals(category.getMax(), 200);
          break;
        case SUCCESS:
          assertEquals(category.getCategory(), "2xx");
          assertEquals(category.getMin(), 200);
          assertEquals(category.getMax(), 300);
          break;
        case REDIRECTION:
          assertEquals(category.getCategory(), "3xx");
          assertEquals(category.getMin(), 300);
          assertEquals(category.getMax(), 400);
          break;
        case CLIENT_ERROR:
          assertEquals(category.getCategory(), "4xx");
          assertEquals(category.getMin(), 400);
          assertEquals(category.getMax(), 500);
          break;
        case SERVER_ERROR:
          assertEquals(category.getCategory(), "5xx");
          assertEquals(category.getMin(), 500);
          assertEquals(category.getMax(), 600);
          break;
        case UNKNOWN:
          assertEquals(category.getCategory(), "Unknown");
          assertEquals(category.getMin(), 0);
          assertEquals(category.getMax(), 0);
          break;
        default:
          throw new IllegalArgumentException("Unknown category: " + category);
      }
    }
  }

  @Test
  public void testUnknownCategory() {
    assertEquals(VeniceHttpResponseStatusCodeCategory.valueOf(99), VeniceHttpResponseStatusCodeCategory.UNKNOWN);
    assertNotEquals(VeniceHttpResponseStatusCodeCategory.valueOf(100), VeniceHttpResponseStatusCodeCategory.UNKNOWN);
    assertEquals(VeniceHttpResponseStatusCodeCategory.valueOf(600), VeniceHttpResponseStatusCodeCategory.UNKNOWN);
  }
}
