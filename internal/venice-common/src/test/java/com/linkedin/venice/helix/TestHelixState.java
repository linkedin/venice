package com.linkedin.venice.helix;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;


/**
 * Test validate HelixState.
 */
public class TestHelixState {
  @Test
  public void testValidateHelixState() {
    assertTrue(HelixState.isValidHelixState(HelixState.STANDBY_STATE));
    assertFalse(HelixState.isValidHelixState("start"));
  }
}
