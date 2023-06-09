package com.linkedin.venice.integration.utils;

import org.testng.Assert;
import org.testng.annotations.Test;


public class SystemExitPrevention {
  @Test
  public void testSystemExitPrevention() {
    /**
     * N.B.: We must make sure the JVM loads {@link ServiceFactory}, since the {@link SecurityManager}
     *       is configured statically in there, hence the throw away initialization...
     */
    try (ZkServerWrapper throwAway = ServiceFactory.getZkServer()) {
      Assert.assertThrows(SecurityException.class, () -> System.exit(1));
      Assert.assertThrows(SecurityException.class, () -> System.exit(100));
    }
  }
}
