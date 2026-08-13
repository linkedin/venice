package com.linkedin.venice.meta;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Why a per-version {@link StorageMode} update was requested. Carried as an optional parameter on the
 * update-version-storage-mode controller API so the controller that applies the update can tell an automated
 * fail-open apart from a manual or operational change, without callers having to encode intent in free-form
 * strings.
 *
 * <p>Requests that omit the reason are treated as {@link #UNSPECIFIED}, which keeps older clients working.
 */
public enum VersionStorageModeUpdateReason {
  /** Default. A manual or otherwise unattributed storage-mode change. Not alertable. */
  UNSPECIFIED,

  /**
   * The push job exhausted its external-storage write retries in the targeted region(s) and is failing open by
   * downgrading those regions' version storage mode back to {@link StorageMode#INTERNAL} before end of push. The
   * push still succeeds, so this is the only signal that the region lost its external-storage copy, and the
   * controller applying it emits an alertable metric.
   */
  EXTERNAL_WRITE_FAILURE;

  private static final Logger LOGGER = LogManager.getLogger(VersionStorageModeUpdateReason.class);

  /**
   * Parse a reason supplied over the controller API, tolerating {@code null} and blank values so that a newer
   * client talking to an older controller — or the reverse — never fails the request over telemetry intent. An
   * unrecognized non-blank value is logged, since it typically means a caller mis-typed the enum name and would
   * otherwise silently lose the alertable signal this reason exists to carry.
   *
   * @return the matching reason, or {@link #UNSPECIFIED} when the value is absent or unrecognized
   */
  public static VersionStorageModeUpdateReason parseOrDefault(String value) {
    if (value == null || value.trim().isEmpty()) {
      return UNSPECIFIED;
    }
    String trimmedValue = value.trim();
    for (VersionStorageModeUpdateReason reason: values()) {
      if (reason.name().equalsIgnoreCase(trimmedValue)) {
        return reason;
      }
    }
    LOGGER.warn("Unrecognized VersionStorageModeUpdateReason value '{}', defaulting to {}", trimmedValue, UNSPECIFIED);
    return UNSPECIFIED;
  }
}
