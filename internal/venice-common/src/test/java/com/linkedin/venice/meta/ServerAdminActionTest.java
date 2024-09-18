package com.linkedin.venice.meta;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.expectThrows;

import org.testng.annotations.Test;


public class ServerAdminActionTest {
  @Test
  public void testFromValue() {
    // Test all valid values
    assertEquals(ServerAdminAction.fromValue(0), ServerAdminAction.DUMP_INGESTION_STATE);
    assertEquals(ServerAdminAction.fromValue(1), ServerAdminAction.DUMP_SERVER_CONFIGS);

    // Test invalid values
    expectThrows(IllegalArgumentException.class, () -> ServerAdminAction.fromValue(-1));
    expectThrows(IllegalArgumentException.class, () -> ServerAdminAction.fromValue(2));
  }
}
