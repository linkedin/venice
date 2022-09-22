package com.linkedin.alpini.netty4.handlers;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Created by acurtis on 9/20/17.
 */
public class TestStaleConnectionHandler {

  // TODO more comprehensive test using EmbeddedChannel
  /**
   * Test the close time calculation for active connections. Should close connections between 5 and 6 hours old by default.
   */
  @Test(groups = "unit")
  public void testDefaultCloseTimeCalculation() {
    long[] calculatedCloseTimes = new long[1000];
    // min calculated time that any channel should be closed is 6 hours from now (minus 1 second for slop)
    long expectedMinCloseTime = System.currentTimeMillis() + TimeUnit.MILLISECONDS.convert(6, TimeUnit.HOURS) - 1000;

    // Simulate 1000 channel connections and get the calculated close times for each connection.
    // The times should be >= 6 hours from now, and <= 12 hours from now, and should have some randomness to them.
    for (int i = 0; i < calculatedCloseTimes.length; i++) {
      StaleConnectionHandler handler = new StaleConnectionHandler(6, 12, TimeUnit.HOURS);
      calculatedCloseTimes[i] = handler.getCloseConnectionTimeMillis();
    }
    // max calculated time that any channel should be closed is 12 hours from now (plus 1 second for slop)
    long expectedMaxCloseTime = System.currentTimeMillis() + TimeUnit.MILLISECONDS.convert(12, TimeUnit.HOURS) + 1000;

    for (long time: calculatedCloseTimes) {
      Assert.assertTrue(
          time >= expectedMinCloseTime,
          "Calculated close time " + time + " is " + (expectedMinCloseTime - time) + "ms before expectedMinCloseTime "
              + expectedMinCloseTime);
      Assert.assertTrue(
          time <= expectedMaxCloseTime,
          "Calculated close time " + time + " is " + (time - expectedMaxCloseTime) + "ms after expectedMaxCloseTime "
              + expectedMaxCloseTime);
    }

    // Make sure we generated times that were not all the same. The handler should choose times between 6 and 12 hours
    // from now, so we expect at least
    // one hour of variation amongst our 1000 samples.
    Arrays.sort(calculatedCloseTimes);
    Assert.assertTrue(
        calculatedCloseTimes[calculatedCloseTimes.length - 1] - calculatedCloseTimes[0] > TimeUnit.MILLISECONDS
            .convert(1, TimeUnit.HOURS),
        "Expected at least 1 minute of variance between calculated close times. Variance was only "
            + (calculatedCloseTimes[calculatedCloseTimes.length - 1] - calculatedCloseTimes[0]) + "ms");
  }

  /**
   * Make sure the constructor validates as expected
   */
  @Test(groups = "unit", expectedExceptions = IllegalArgumentException.class)
  public void testConstructorValidation() {
    new StaleConnectionHandler(2, 1, TimeUnit.SECONDS);
    Assert.fail("Expected IllegalArgumentException with minAge=2 > maxAge = 1");
  }

  /**
   * Make sure the constructor validates as expected
   */
  @Test(groups = "unit")
  public void testConstructorValidation2() {
    new StaleConnectionHandler();
  }
}
