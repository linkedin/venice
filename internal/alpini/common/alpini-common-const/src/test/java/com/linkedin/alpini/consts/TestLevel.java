package com.linkedin.alpini.consts;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Created by acurtis on 12/6/17.
 */
public class TestLevel {
  private final Logger _log = LogManager.getLogger(getClass());

  @Test(groups = "unit")
  public void testSetLevel() {

    Level.setLevel(_log, Level.DEBUG);

    Assert.assertTrue(Level.isEnabledFor(_log, Level.DEBUG));
    Assert.assertEquals(Level.getLevel(_log), Level.DEBUG);

    Assert.assertEquals(LogManager.getLogger(getClass()).getLevel(), org.apache.logging.log4j.Level.DEBUG);

    Level.setLevel(_log, Level.INFO);

    Assert.assertTrue(Level.isEnabledFor(_log, Level.INFO));
    Assert.assertFalse(Level.isEnabledFor(_log, Level.DEBUG));
    Assert.assertEquals(Level.getLevel(_log), Level.INFO);

    Assert.assertEquals(LogManager.getLogger(getClass()).getLevel(), org.apache.logging.log4j.Level.INFO);

    Level.setLevel(_log, Level.WARN);

    Assert.assertTrue(Level.isEnabledFor(_log, Level.WARN));
    Assert.assertFalse(Level.isEnabledFor(_log, Level.INFO));
    Assert.assertEquals(Level.getLevel(_log), Level.WARN);

    Assert.assertEquals(LogManager.getLogger(getClass()).getLevel(), org.apache.logging.log4j.Level.WARN);

    Level.setLevel(_log, Level.ERROR);

    Assert.assertTrue(Level.isEnabledFor(_log, Level.ERROR));
    Assert.assertFalse(Level.isEnabledFor(_log, Level.INFO));
    Assert.assertEquals(Level.getLevel(_log), Level.ERROR);

    Assert.assertEquals(LogManager.getLogger(getClass()).getLevel(), org.apache.logging.log4j.Level.ERROR);

    Level.setLevel(_log, Level.OFF);
    Assert.assertFalse(Level.isEnabledFor(_log, Level.ERROR));
  }
}
