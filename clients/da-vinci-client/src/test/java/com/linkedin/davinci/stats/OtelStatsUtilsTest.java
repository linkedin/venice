package com.linkedin.davinci.stats;

import static com.linkedin.venice.meta.Store.NON_EXISTING_VERSION;
import static org.testng.Assert.assertEquals;

import com.linkedin.davinci.stats.OtelStatsUtils.VersionInfo;
import com.linkedin.venice.server.VersionRole;
import org.testng.annotations.Test;


/**
 * Unit tests for {@link OtelStatsUtils}.
 */
public class OtelStatsUtilsTest {
  @Test
  public void testVersionInfoGetters() {
    VersionInfo versionInfo = new VersionInfo(5, 6);
    assertEquals(versionInfo.getCurrentVersion(), 5);
    assertEquals(versionInfo.getFutureVersion(), 6);
  }

  @Test
  public void testVersionInfoWithNonExistingVersions() {
    VersionInfo versionInfo = new VersionInfo(NON_EXISTING_VERSION, NON_EXISTING_VERSION);
    assertEquals(versionInfo.getCurrentVersion(), NON_EXISTING_VERSION);
    assertEquals(versionInfo.getFutureVersion(), NON_EXISTING_VERSION);
  }

  @Test
  public void testClassifyVersionAsCurrent() {
    VersionInfo versionInfo = new VersionInfo(5, 6);
    assertEquals(OtelStatsUtils.classifyVersion(5, versionInfo), VersionRole.CURRENT);
  }

  @Test
  public void testClassifyVersionAsFuture() {
    VersionInfo versionInfo = new VersionInfo(5, 6);
    assertEquals(OtelStatsUtils.classifyVersion(6, versionInfo), VersionRole.FUTURE);
  }

  @Test
  public void testClassifyVersionAsBackup() {
    VersionInfo versionInfo = new VersionInfo(5, 6);
    // Any version that is neither current nor future should be BACKUP
    assertEquals(OtelStatsUtils.classifyVersion(4, versionInfo), VersionRole.BACKUP);
    assertEquals(OtelStatsUtils.classifyVersion(7, versionInfo), VersionRole.BACKUP);
    assertEquals(OtelStatsUtils.classifyVersion(0, versionInfo), VersionRole.BACKUP);
  }

  @Test
  public void testClassifyVersionWithNonExistingVersions() {
    // When current and future are both NON_EXISTING_VERSION, all versions should be BACKUP
    VersionInfo versionInfo = new VersionInfo(NON_EXISTING_VERSION, NON_EXISTING_VERSION);
    assertEquals(OtelStatsUtils.classifyVersion(1, versionInfo), VersionRole.BACKUP);
    assertEquals(OtelStatsUtils.classifyVersion(5, versionInfo), VersionRole.BACKUP);
  }

  @Test
  public void testClassifyVersionWhenCurrentEqualsNonExisting() {
    // Version matching NON_EXISTING_VERSION should be classified as CURRENT if that's the current version
    VersionInfo versionInfo = new VersionInfo(NON_EXISTING_VERSION, 5);
    assertEquals(OtelStatsUtils.classifyVersion(NON_EXISTING_VERSION, versionInfo), VersionRole.CURRENT);
    assertEquals(OtelStatsUtils.classifyVersion(5, versionInfo), VersionRole.FUTURE);
  }

  @Test
  public void testClassifyVersionWhenOnlyCurrentExists() {
    // Only current version exists, future is NON_EXISTING_VERSION
    VersionInfo versionInfo = new VersionInfo(5, NON_EXISTING_VERSION);
    assertEquals(OtelStatsUtils.classifyVersion(5, versionInfo), VersionRole.CURRENT);
    assertEquals(OtelStatsUtils.classifyVersion(6, versionInfo), VersionRole.BACKUP);
  }

  @Test
  public void testClassifyVersionWhenCurrentEqualsFuture() {
    // Edge case: current equals future (should classify as CURRENT due to if-else order)
    VersionInfo versionInfo = new VersionInfo(5, 5);
    assertEquals(OtelStatsUtils.classifyVersion(5, versionInfo), VersionRole.CURRENT);
  }
}
