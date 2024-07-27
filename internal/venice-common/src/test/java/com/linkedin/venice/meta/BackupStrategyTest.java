package com.linkedin.venice.meta;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

import com.linkedin.venice.exceptions.VeniceException;
import org.testng.annotations.Test;


public class BackupStrategyTest {
  @Test
  public void testFromInt() {
    assertEquals(BackupStrategy.fromInt(0), BackupStrategy.KEEP_MIN_VERSIONS);
    assertEquals(BackupStrategy.fromInt(1), BackupStrategy.DELETE_ON_NEW_PUSH_START);
    assertThrows(VeniceException.class, () -> BackupStrategy.fromInt(2));
  }

  @Test
  public void testGetValue() {
    assertEquals(BackupStrategy.KEEP_MIN_VERSIONS.getValue(), 0);
    assertEquals(BackupStrategy.DELETE_ON_NEW_PUSH_START.getValue(), 1);
  }
}
