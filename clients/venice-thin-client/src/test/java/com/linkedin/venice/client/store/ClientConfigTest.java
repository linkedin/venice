package com.linkedin.venice.client.store;

import org.testng.Assert;
import org.testng.annotations.Test;


public class ClientConfigTest {
  @Test
  public void testCloneConfig() {
    ClientConfig config = new ClientConfig("Test-store");
    ClientConfig clonedConfig = ClientConfig.cloneConfig(config);

    Assert.assertEquals(config, clonedConfig);
  }

  @Test
  public void testStatTrackingEnabled() {
    ClientConfig config = new ClientConfig("Test-store").setStatTrackingEnabled(true);
    Assert.assertTrue(config.isStatTrackingEnabled());
    Assert.assertTrue(ClientConfig.cloneConfig(config).isStatTrackingEnabled());
  }
}
