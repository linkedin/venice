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
}
