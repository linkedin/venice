package com.linkedin.venice.tests;

import org.testng.Assert;
import org.testng.annotations.Test;


public class SystemExitPreventionByGradle {
  /**
   * This test verifies that the Gradle-level safeguard against {@link System#exit(int)} works. If running
   * from the IDE or some other environment that isn't the Venice build, it is expected that it wouldn't
   * work.
   */
  @Test
  public void testSystemExitPrevention() {
    Assert.assertThrows(SecurityException.class, () -> System.exit(1));
    Assert.assertThrows(SecurityException.class, () -> System.exit(100));
  }
}
