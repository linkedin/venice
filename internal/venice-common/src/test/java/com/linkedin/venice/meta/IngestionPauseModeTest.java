package com.linkedin.venice.meta;

import com.linkedin.venice.exceptions.VeniceException;
import org.testng.Assert;
import org.testng.annotations.Test;


public class IngestionPauseModeTest {
  @Test
  public void testFromInt() {
    Assert.assertEquals(IngestionPauseMode.fromInt(0), IngestionPauseMode.NOT_PAUSED);
    Assert.assertEquals(IngestionPauseMode.fromInt(1), IngestionPauseMode.CURRENT_VERSION);
    Assert.assertEquals(IngestionPauseMode.fromInt(2), IngestionPauseMode.ALL_VERSIONS);
  }

  @Test
  public void testGetValue() {
    for (IngestionPauseMode mode: IngestionPauseMode.values()) {
      Assert.assertEquals(IngestionPauseMode.fromInt(mode.getValue()), mode);
    }
  }

  @Test(expectedExceptions = VeniceException.class)
  public void testFromIntInvalid() {
    IngestionPauseMode.fromInt(99);
  }

  @Test
  public void testDefaultIsNotPaused() {
    Assert.assertEquals(IngestionPauseMode.fromInt(0), IngestionPauseMode.NOT_PAUSED);
  }
}
