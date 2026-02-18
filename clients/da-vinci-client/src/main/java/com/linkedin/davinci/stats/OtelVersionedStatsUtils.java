package com.linkedin.davinci.stats;

import com.linkedin.venice.server.VersionRole;


/**
 * Shared utilities for OpenTelemetry versioned stats classes.
 * These utilities are used by {@link com.linkedin.davinci.stats.ingestion.IngestionOtelStats}
 * and {@link com.linkedin.davinci.stats.ingestion.heartbeat.HeartbeatOtelStats}.
 */
public class OtelVersionedStatsUtils {
  private OtelVersionedStatsUtils() {
    // Utility class, not meant to be instantiated
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
   *
   * @param version The version number to classify
   * @param versionInfo The current/future version info
   * @return {@link VersionRole#CURRENT} if version matches currentVersion,
   *         {@link VersionRole#FUTURE} if version matches futureVersion,
   *         {@link VersionRole#BACKUP} otherwise
   */
  public static VersionRole classifyVersion(int version, VersionInfo versionInfo) {
    if (version == versionInfo.currentVersion) {
      return VersionRole.CURRENT;
    } else if (version == versionInfo.futureVersion) {
      return VersionRole.FUTURE;
    }
    return VersionRole.BACKUP;
  }
}
