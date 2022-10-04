/*
 * $Id$
 */
package com.linkedin.alpini.base.misc;

import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * @author Jemiah Westerman &lt;jwesterman@linkedin.com&gt;
 *
 * @version $Revision$
 */
public class TestTimeFormat {
  @Test(groups = "unit")
  public void testFormatAndParseDatetime() {
    // Test times, starting at some time and going forward 2 days in 1500ms increments
    long baseTime = 1445368400332L;
    for (int i = 0; i < 1000 * 60 * 60 * 24 * 2; i += 1500) {
      long time = baseTime + i;
      String s = TimeFormat.formatDatetime(time);
      long parsedMillis = TimeFormat.parseDatetimeToMillis(s);
      Assert.assertEquals(
          parsedMillis,
          time,
          "Failed to format and parse date time " + time + " milliseconds. " + " Formatted value was (" + s
              + "). Parsed millis was (" + parsedMillis + ").");
    }
  }

  @Test(groups = "unit")
  public void testParseDatetimeErrors() {
    try {
      TimeFormat.parseDatetimeToMillis("");
      Assert.fail("Should have raised IllegalArgumentException");
    } catch (IllegalArgumentException ex) {
      // good
    }

    try {
      TimeFormat.parseDatetimeToMillis(null);
      Assert.fail("Should have raised IllegalArgumentException");
    } catch (IllegalArgumentException ex) {
      // good
    }

    try {
      TimeFormat.parseDatetimeToMillis("113123");
      Assert.fail("Should have raised IllegalArgumentException");
    } catch (IllegalArgumentException ex) {
      // good
    }
  }

  @Test(groups = "unit")
  public void testFormatZeroTimespan() {
    String s = TimeFormat.formatTimespan(0);
    Assert.assertEquals(s, "0ms");
  }

  @Test(groups = "unit")
  public void testFormatNegativeTimespan() {
    String s = TimeFormat.formatTimespan(-5);
    Assert.assertEquals(s, "-5ms");
  }

  @Test(groups = "unit")
  public void testFormatTimespanHappyPath() {
    String s;

    s = TimeFormat.formatTimespan(1);
    Assert.assertEquals(s, "1ms");

    s = TimeFormat.formatTimespan(1000);
    Assert.assertEquals(s, "1s");

    s = TimeFormat.formatTimespan(1001);
    Assert.assertEquals(s, "1s1ms");

    s = TimeFormat.formatTimespan(1000 * 60);
    Assert.assertEquals(s, "1m");

    s = TimeFormat.formatTimespan(1000 * 60 * 60);
    Assert.assertEquals(s, "1h");

    s = TimeFormat.formatTimespan(1000 * 60 * 60 * 24);
    Assert.assertEquals(s, "1d");

    s = TimeFormat.formatTimespan(1000 * 60 * 60 * 24 + // 1 day
        1000 * 60 * 60 * 2 + // 2 hours
        1000 * 60 * 15 + // 15 minutes
        1000 * 59 + // 59 seconds
        999); // 999 millis

    Assert.assertEquals(s, "1d2h15m59s999ms");
  }

  @Test(groups = "unit")
  public void testParseZeroTimespan() {
    long millis = TimeFormat.parseTimespanToMillis("0ms");
    Assert.assertEquals(millis, 0);
  }

  @Test(groups = "unit")
  public void testParseNegativeTimespan() {
    long millis = TimeFormat.parseTimespanToMillis("-5ms");
    Assert.assertEquals(millis, -5);
  }

  @Test(groups = "unit")
  public void testParseTimespanHappyPath() {
    long expectedMillis = 1000 * 60 * 60 * 24 + // 1 day
        1000 * 60 * 60 * 2 + // 2 hours
        1000 * 60 * 15 + // 15 minutes
        1000 * 59 + // 59 seconds
        999; // 999 millis

    long actualMillis = TimeFormat.parseTimespanToMillis("1d2h15m59s999ms");
    Assert.assertEquals(actualMillis, expectedMillis);
  }

  @Test(groups = "unit")
  public void testParseTimespanErrors() {
    try {
      TimeFormat.parseTimespanToMillis("");
      Assert.fail("Should have raised IllegalArgumentException");
    } catch (IllegalArgumentException ex) {
      // good
    }

    try {
      TimeFormat.parseTimespanToMillis(null);
      Assert.fail("Should have raised IllegalArgumentException");
    } catch (IllegalArgumentException ex) {
      // good
    }

    try {
      TimeFormat.parseTimespanToMillis("1");
      Assert.fail("Should have raised IllegalArgumentException");
    } catch (IllegalArgumentException ex) {
      // good
    }

    try {
      TimeFormat.parseTimespanToMillis("1f");
      Assert.fail("Should have raised IllegalArgumentException");
    } catch (IllegalArgumentException ex) {
      // good
    }
    try {
      TimeFormat.parseTimespanToMillis("1d7");
      Assert.fail("Should have raised IllegalArgumentException");
    } catch (IllegalArgumentException ex) {
      // good
    }
    try {
      TimeFormat.parseTimespanToMillis("1d7s^m");
      Assert.fail("Should have raised IllegalArgumentException");
    } catch (IllegalArgumentException ex) {
      // good
    }
    try {
      // Huge number matches the regex, but triggers NumberFormatException when converting to int.
      TimeFormat.parseTimespanToMillis("1d99999999999999999999999999999999999999999999999999s");
      Assert.fail("Should have raised IllegalArgumentException");
    } catch (IllegalArgumentException ex) {
      // good
    }
  }

  @Test(groups = "unit")
  public void testFormatAndParseTimespan() {
    // Test times from 0 ms to 2 days in 1500ms increments
    for (int i = 0; i < 1000 * 60 * 60 * 24 * 2; i += 1500) {
      String s = TimeFormat.formatTimespan(i);
      long parsedMillis = TimeFormat.parseTimespanToMillis(s);
      Assert.assertEquals(
          parsedMillis,
          i,
          "Failed to format and parse " + i + " millisecond timespan. " + " Formatted value was (" + s
              + "). Parsed millis was (" + parsedMillis + ").");
    }
  }

}
