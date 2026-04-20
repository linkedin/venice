package com.linkedin.davinci.stats;

import static com.linkedin.venice.meta.Store.NON_EXISTING_VERSION;
import static org.testng.Assert.assertEquals;

import com.linkedin.davinci.stats.OtelVersionedStatsUtils.VersionInfo;
import com.linkedin.venice.server.VersionRole;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.testng.annotations.Test;


/**
 * Unit tests for {@link OtelVersionedStatsUtils}.
 */
public class OtelVersionedStatsUtilsTest {
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
    assertEquals(OtelVersionedStatsUtils.classifyVersion(5, versionInfo), VersionRole.CURRENT);
  }

  @Test
  public void testClassifyVersionAsFuture() {
    VersionInfo versionInfo = new VersionInfo(5, 6);
    assertEquals(OtelVersionedStatsUtils.classifyVersion(6, versionInfo), VersionRole.FUTURE);
  }

  @Test
  public void testClassifyVersionAsBackup() {
    VersionInfo versionInfo = new VersionInfo(5, 6);
    // Any version that is neither current nor future should be BACKUP
    assertEquals(OtelVersionedStatsUtils.classifyVersion(4, versionInfo), VersionRole.BACKUP);
    assertEquals(OtelVersionedStatsUtils.classifyVersion(7, versionInfo), VersionRole.BACKUP);
    assertEquals(OtelVersionedStatsUtils.classifyVersion(0, versionInfo), VersionRole.BACKUP);
  }

  @Test
  public void testClassifyVersionWithNonExistingVersions() {
    // When current and future are both NON_EXISTING_VERSION, all versions should be BACKUP
    VersionInfo versionInfo = new VersionInfo(NON_EXISTING_VERSION, NON_EXISTING_VERSION);
    assertEquals(OtelVersionedStatsUtils.classifyVersion(1, versionInfo), VersionRole.BACKUP);
    assertEquals(OtelVersionedStatsUtils.classifyVersion(5, versionInfo), VersionRole.BACKUP);
  }

  @Test
  public void testClassifyVersionWhenCurrentEqualsNonExisting() {
    // Version matching NON_EXISTING_VERSION should be classified as CURRENT if that's the current version
    VersionInfo versionInfo = new VersionInfo(NON_EXISTING_VERSION, 5);
    assertEquals(OtelVersionedStatsUtils.classifyVersion(NON_EXISTING_VERSION, versionInfo), VersionRole.CURRENT);
    assertEquals(OtelVersionedStatsUtils.classifyVersion(5, versionInfo), VersionRole.FUTURE);
  }

  @Test
  public void testClassifyVersionWhenOnlyCurrentExists() {
    // Only current version exists, future is NON_EXISTING_VERSION
    VersionInfo versionInfo = new VersionInfo(5, NON_EXISTING_VERSION);
    assertEquals(OtelVersionedStatsUtils.classifyVersion(5, versionInfo), VersionRole.CURRENT);
    assertEquals(OtelVersionedStatsUtils.classifyVersion(6, versionInfo), VersionRole.BACKUP);
  }

  @Test
  public void testClassifyVersionWhenCurrentEqualsFuture() {
    // Edge case: current equals future (should classify as CURRENT due to if-else order)
    VersionInfo versionInfo = new VersionInfo(5, 5);
    assertEquals(OtelVersionedStatsUtils.classifyVersion(5, versionInfo), VersionRole.CURRENT);
  }

  // --- getVersionForRole tests ---

  @Test
  public void testResolveCurrentVersion() {
    VersionInfo info = new VersionInfo(5, 6);
    Set<Integer> known = new HashSet<>(Arrays.asList(4, 5, 6));
    assertEquals(OtelVersionedStatsUtils.getVersionForRole(VersionRole.CURRENT, info, known), 5);
  }

  @Test
  public void testResolveFutureVersion() {
    VersionInfo info = new VersionInfo(5, 6);
    Set<Integer> known = new HashSet<>(Arrays.asList(4, 5, 6));
    assertEquals(OtelVersionedStatsUtils.getVersionForRole(VersionRole.FUTURE, info, known), 6);
  }

  @Test
  public void testResolveBackupSelectsSmallest() {
    VersionInfo info = new VersionInfo(5, 6);
    Set<Integer> known = new HashSet<>(Arrays.asList(1, 3, 4, 5, 6));
    assertEquals(OtelVersionedStatsUtils.getVersionForRole(VersionRole.BACKUP, info, known), 1);
  }

  @Test
  public void testResolveBackupEmptyKnownVersions() {
    VersionInfo info = new VersionInfo(5, 6);
    assertEquals(
        OtelVersionedStatsUtils.getVersionForRole(VersionRole.BACKUP, info, Collections.emptySet()),
        NON_EXISTING_VERSION);
  }

  @Test
  public void testResolveBackupAllVersionsAreCurrentOrFuture() {
    VersionInfo info = new VersionInfo(5, 6);
    Set<Integer> known = new HashSet<>(Arrays.asList(5, 6));
    assertEquals(OtelVersionedStatsUtils.getVersionForRole(VersionRole.BACKUP, info, known), NON_EXISTING_VERSION);
  }

  @Test
  public void testResolveVersionWithNullVersionInfo() {
    Set<Integer> known = new HashSet<>(Arrays.asList(1, 2, 3));
    assertEquals(OtelVersionedStatsUtils.getVersionForRole(VersionRole.CURRENT, null, known), NON_EXISTING_VERSION);
    assertEquals(OtelVersionedStatsUtils.getVersionForRole(VersionRole.FUTURE, null, known), NON_EXISTING_VERSION);
    assertEquals(OtelVersionedStatsUtils.getVersionForRole(VersionRole.BACKUP, null, known), NON_EXISTING_VERSION);
  }

  @Test
  public void testResolveCurrentReturnsNonExistingWhenUnset() {
    VersionInfo info = new VersionInfo(NON_EXISTING_VERSION, 6);
    Set<Integer> known = new HashSet<>(Arrays.asList(1, 6));
    assertEquals(OtelVersionedStatsUtils.getVersionForRole(VersionRole.CURRENT, info, known), NON_EXISTING_VERSION);
  }

  @Test
  public void testClassifyVersionWithNullVersionInfoReturnsBackup() {
    // classifyVersion is null-safe and defaults to BACKUP when versionInfo is null
    assertEquals(OtelVersionedStatsUtils.classifyVersion(1, null), VersionRole.BACKUP);
    assertEquals(OtelVersionedStatsUtils.classifyVersion(5, null), VersionRole.BACKUP);
  }

  @Test
  public void testVersionRoleEnumCount() {
    // getVersionForRole returns NON_EXISTING_VERSION for unknown roles — but a new VersionRole
    // value means the switch should be updated to handle it explicitly.
    assertEquals(
        VersionRole.values().length,
        3,
        "New VersionRole value added — update getVersionForRole switch in OtelVersionedStatsUtils");
  }
}
