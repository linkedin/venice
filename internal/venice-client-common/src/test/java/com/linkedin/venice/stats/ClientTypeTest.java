package com.linkedin.venice.stats;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;


public class ClientTypeTest {
  @Test
  public void testClientType() {
    assertEquals(ClientType.THIN_CLIENT.getName(), "thin-client");
    assertEquals(ClientType.FAST_CLIENT.getName(), "fast-client");
    assertEquals(ClientType.DAVINCI_CLIENT.getName(), "davinci-client");

    assertEquals(ClientType.THIN_CLIENT.getMetricsPrefix(), "thin_client");
    assertEquals(ClientType.FAST_CLIENT.getMetricsPrefix(), "fast_client");
    assertEquals(ClientType.DAVINCI_CLIENT.getMetricsPrefix(), "davinci_client");

    assertTrue(ClientType.isDavinciClient(ClientType.DAVINCI_CLIENT));
    assertFalse(ClientType.isDavinciClient(ClientType.THIN_CLIENT));
    assertFalse(ClientType.isDavinciClient(ClientType.FAST_CLIENT));
  }
}
