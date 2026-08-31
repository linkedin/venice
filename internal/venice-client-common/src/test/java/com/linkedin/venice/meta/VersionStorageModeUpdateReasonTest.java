package com.linkedin.venice.meta;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;


public class VersionStorageModeUpdateReasonTest {
  @Test
  public void testParseOrDefaultWithNull() {
    assertEquals(VersionStorageModeUpdateReason.parseOrDefault(null), VersionStorageModeUpdateReason.UNSPECIFIED);
  }

  @Test
  public void testParseOrDefaultWithEmptyOrBlank() {
    assertEquals(VersionStorageModeUpdateReason.parseOrDefault(""), VersionStorageModeUpdateReason.UNSPECIFIED);
    assertEquals(VersionStorageModeUpdateReason.parseOrDefault("   "), VersionStorageModeUpdateReason.UNSPECIFIED);
  }

  @Test
  public void testParseOrDefaultWithKnownValue() {
    assertEquals(
        VersionStorageModeUpdateReason.parseOrDefault("EXTERNAL_WRITE_FAILURE"),
        VersionStorageModeUpdateReason.EXTERNAL_WRITE_FAILURE);
  }

  @Test
  public void testParseOrDefaultIsCaseInsensitiveAndTrims() {
    assertEquals(
        VersionStorageModeUpdateReason.parseOrDefault("  external_write_failure  "),
        VersionStorageModeUpdateReason.EXTERNAL_WRITE_FAILURE);
  }

  @Test
  public void testParseOrDefaultWithUnrecognizedValueLogsAndDefaults() {
    // A typo'd reason (e.g. "EXTERNAL_WRITE_FAILUR") must not be silently mistaken for a recognized reason; it
    // has to resolve to UNSPECIFIED just like a genuinely unspecified request.
    assertEquals(
        VersionStorageModeUpdateReason.parseOrDefault("EXTERNAL_WRITE_FAILUR"),
        VersionStorageModeUpdateReason.UNSPECIFIED);
  }
}
