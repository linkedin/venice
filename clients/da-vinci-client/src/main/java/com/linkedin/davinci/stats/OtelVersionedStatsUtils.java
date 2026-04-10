package com.linkedin.davinci.stats;

import static com.linkedin.venice.meta.Store.NON_EXISTING_VERSION;

import com.linkedin.venice.server.VersionRole;
import java.util.Set;


/**
 * Shared utilities for OpenTelemetry versioned stats classes that classify and resolve version roles.
 */
public class OtelVersionedStatsUtils {
  private OtelVersionedStatsUtils() {
  }

  /**
   * Immutable holder for current and future version numbers.
   * Used to classify versions as CURRENT, FUTURE, or BACKUP.
   */
  public static class VersionInfo {
    private final int currentVersion;
    private final int futureVersion;

    public VersionInfo(int currentVersion, int futureVersion) {
      this.currentVersion = currentVersion;
      this.futureVersion = futureVersion;
    }

    public int getCurrentVersion() {
      return currentVersion;
    }

    public int getFutureVersion() {
      return futureVersion;
    }
  }

  /**
   * Classifies a version as CURRENT, FUTURE, or BACKUP.
   * Returns {@link VersionRole#BACKUP} when {@code versionInfo} is null (e.g., store
   * not yet registered in a per-store version info map).
   *
   * @param version The version number to classify
   * @param versionInfo The current/future version info, or null
   * @return {@link VersionRole#CURRENT} if version matches currentVersion,
   *         {@link VersionRole#FUTURE} if version matches futureVersion,
   *         {@link VersionRole#BACKUP} otherwise or if versionInfo is null
   */
  public static VersionRole classifyVersion(int version, VersionInfo versionInfo) {
    if (versionInfo == null) {
      return VersionRole.BACKUP;
    }
    if (version == versionInfo.getCurrentVersion()) {
      return VersionRole.CURRENT;
    } else if (version == versionInfo.getFutureVersion()) {
      return VersionRole.FUTURE;
    }
    return VersionRole.BACKUP;
  }

  /**
   * Resolves a {@link VersionRole} to a version number using the given {@link VersionInfo}
   * and a set of known version numbers. For {@link VersionRole#BACKUP}, returns the smallest
   * version that is neither current nor future (deterministic selection from an unordered set).
   *
   * @param role The version role to resolve
   * @param versionInfo The current/future version info (read once from volatile before calling)
   * @param knownVersions The set of known version numbers (e.g., from a per-version map's keySet)
   * @return The version number, or {@link com.linkedin.venice.meta.Store#NON_EXISTING_VERSION} if
   *         versionInfo is null, no backup version exists, or the role is not recognized
   */
  public static int getVersionForRole(VersionRole role, VersionInfo versionInfo, Set<Integer> knownVersions) {
    if (versionInfo == null) {
      return NON_EXISTING_VERSION;
    }
    switch (role) {
      case CURRENT:
        return versionInfo.getCurrentVersion();
      case FUTURE:
        return versionInfo.getFutureVersion();
      case BACKUP:
        int backupVersion = NON_EXISTING_VERSION;
        for (Integer version: knownVersions) {
          if (version != versionInfo.getCurrentVersion() && version != versionInfo.getFutureVersion()) {
            if (backupVersion == NON_EXISTING_VERSION || version < backupVersion) {
              backupVersion = version;
            }
          }
        }
        return backupVersion;
      default:
        return NON_EXISTING_VERSION;
    }
  }
}
