package com.linkedin.davinci.config;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

import com.linkedin.venice.exceptions.VeniceException;
import org.testng.annotations.Test;


public class NearlineLatencyTimestampSourceTest {
  @Test
  public void testParseAcceptsExactNames() {
    assertEquals(NearlineLatencyTimestampSource.parse("BROKER"), NearlineLatencyTimestampSource.BROKER);
    assertEquals(NearlineLatencyTimestampSource.parse("PRODUCER"), NearlineLatencyTimestampSource.PRODUCER);
  }

  @Test
  public void testParseIsCaseInsensitiveAndTrimsWhitespace() {
    assertEquals(NearlineLatencyTimestampSource.parse("broker"), NearlineLatencyTimestampSource.BROKER);
    assertEquals(NearlineLatencyTimestampSource.parse("Producer"), NearlineLatencyTimestampSource.PRODUCER);
    assertEquals(NearlineLatencyTimestampSource.parse("  broker  "), NearlineLatencyTimestampSource.BROKER);
  }

  @Test
  public void testParseNullDefaultsToBroker() {
    assertEquals(NearlineLatencyTimestampSource.parse(null), NearlineLatencyTimestampSource.BROKER);
  }

  @Test
  public void testParseRejectsUnknownValue() {
    assertThrows(VeniceException.class, () -> NearlineLatencyTimestampSource.parse("invalid"));
    assertThrows(VeniceException.class, () -> NearlineLatencyTimestampSource.parse(""));
  }
}
