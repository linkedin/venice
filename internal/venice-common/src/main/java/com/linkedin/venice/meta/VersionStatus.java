package com.linkedin.venice.meta;

import com.linkedin.venice.exceptions.VeniceException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;


/**
 * Enums of status of verion.
 */
public enum VersionStatus {
  NOT_CREATED(0), STARTED(1),
  // Version has been pushed to venice(offline job related to this version has been completed), but is not ready to
  // serve read request because the writes to this store is disabled.
  PUSHED(2),
  // Version has been pushed to venice and is ready to serve read request.
  ONLINE(3), ERROR(4),
  // Version is created and persisted inside ZK, but controller hasn't finished preparation works yet.
  CREATED(5),
  // Version has been pushed to Venice and is ready to serve in some regions, but failed in other regions.
  // This version status only exists in parent.
  PARTIALLY_ONLINE(6),
  // This version has been killed.
  KILLED(7);

  private final int value;

  VersionStatus(int v) {
    this.value = v;
  }

  private static final Map<Integer, VersionStatus> idMapping = new HashMap<>();
  static {
    Arrays.stream(values()).forEach(v -> idMapping.put(v.value, v));
  }

  public static VersionStatus getVersionStatusFromInt(int v) {
    VersionStatus s = idMapping.get(v);
    if (s == null) {
      throw new VeniceException("Invalid VersionStatus id: " + v);
    }
    return s;
  }

  /**
   * check if a status can be deleted immediately.
   *
   * @param status
   * @return true if it can be deleted immediately, false otherwise
   */
  public static boolean canDelete(VersionStatus status) {
    return status == ERROR || status == KILLED;
  }

  /**
   * For all the status which returns true, last few versions
   * (few count, controlled by config) will be preserved.
   *
   * For a store typically last few online versions should be
   * preserved.
   *
   * @param status
   * @return true if it should be considered, false otherwise
   */
  public static boolean preserveLastFew(VersionStatus status) {
    return ONLINE == status;
  }

  /**
   * Check if the Version has completed the bootstrap. We need to make sure that Kafka topic for uncompleted offline
   * job should NOT be deleted. Otherwise Kafka MM would crash. Attention: For streaming case, even version is ONLINE
   * or PUSHED, it might be not safe to delete kafka topic.
   */
  public static boolean isBootstrapCompleted(VersionStatus status) {
    return status.equals(ONLINE) || status.equals(PUSHED);
  }

  // Check if the version has been killed.
  public static boolean isVersionKilled(VersionStatus status) {
    return status == KILLED;
  }

  // Check if the version is in ERROR state.
  public static boolean isVersionErrored(VersionStatus status) {
    return status == ERROR;
  }
}
