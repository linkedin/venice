package com.linkedin.venice.response;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import java.util.HashSet;
import java.util.Set;
import org.testng.annotations.Test;


public class VeniceReadResponseStatusTest {
  @Test
  public void testGetCode() {
    assertEquals(VeniceReadResponseStatus.KEY_NOT_FOUND.getCode(), -420);
    assertEquals(VeniceReadResponseStatus.OK.getCode(), 200);
    assertEquals(VeniceReadResponseStatus.BAD_REQUEST.getCode(), 400);
    assertEquals(VeniceReadResponseStatus.METHOD_NOT_ALLOWED.getCode(), 405);
    assertEquals(VeniceReadResponseStatus.REQUEST_TIMEOUT.getCode(), 408);
    assertEquals(VeniceReadResponseStatus.MISROUTED_STORE_VERSION.getCode(), 410);
    assertEquals(VeniceReadResponseStatus.TOO_MANY_REQUESTS.getCode(), 429);
    assertEquals(VeniceReadResponseStatus.INTERNAL_ERROR.getCode(), 500);
    assertEquals(VeniceReadResponseStatus.SERVICE_UNAVAILABLE.getCode(), 503);
  }

  @Test
  public void testFromCode() {
    for (VeniceReadResponseStatus status: VeniceReadResponseStatus.values()) {
      VeniceReadResponseStatus resolved = VeniceReadResponseStatus.fromCode(status.getCode());
      assertNotNull(resolved, "fromCode should resolve " + status.name());
      assertEquals(resolved, status);
    }
  }

  @Test
  public void testFromCodeWithUnknownCode() {
    assertNull(VeniceReadResponseStatus.fromCode(999));
    assertNull(VeniceReadResponseStatus.fromCode(0));
    assertNull(VeniceReadResponseStatus.fromCode(-1));
  }

  @Test
  public void testFromCodeWithNegativeCode() {
    VeniceReadResponseStatus status = VeniceReadResponseStatus.fromCode(-420);
    assertNotNull(status);
    assertEquals(status, VeniceReadResponseStatus.KEY_NOT_FOUND);
  }

  @Test
  public void testAllCodesAreUnique() {
    Set<Integer> codes = new HashSet<>();
    for (VeniceReadResponseStatus status: VeniceReadResponseStatus.values()) {
      boolean added = codes.add(status.getCode());
      assertEquals(added, true, "Duplicate code found: " + status.getCode());
    }
  }
}
