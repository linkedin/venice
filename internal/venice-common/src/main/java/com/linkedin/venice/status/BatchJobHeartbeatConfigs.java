package com.linkedin.venice.status;

import com.linkedin.venice.utils.Utils;
import java.time.Duration;


public class BatchJobHeartbeatConfigs {
  public static final Utils.ConfigEntity<String> HEARTBEAT_STORE_CLUSTER_CONFIG = new Utils.ConfigEntity<>(
      "batch.job.heartbeat.store.cluster",
      null,
      "Name of cluster where the batch job liveness heartbeat store should exist");

  public static final Utils.ConfigEntity<Boolean> HEARTBEAT_ENABLED_CONFIG =
      new Utils.ConfigEntity<>("batch.job.heartbeat.enabled", false, "If the heartbeat feature is enabled");
  public static final Utils.ConfigEntity<Long> HEARTBEAT_INTERVAL_CONFIG = new Utils.ConfigEntity<>(
      "batch.job.heartbeat.interval.ms",
      Duration.ofMinutes(1).toMillis(),
      "Time interval between sending two consecutive heartbeats");
  public static final Utils.ConfigEntity<Long> HEARTBEAT_CONTROLLER_TIMEOUT_CONFIG = new Utils.ConfigEntity<>(
      "controller.batch.job.heartbeat.timeout.ms",
      HEARTBEAT_INTERVAL_CONFIG.getDefaultValue() == null
          ? Duration.ofMinutes(3).toMillis()
          : 3 * HEARTBEAT_INTERVAL_CONFIG.getDefaultValue(),
      "The max amount of time the controller waits for a heartbeat to show up before it claims a timeout.");
  public static final Utils.ConfigEntity<Long> HEARTBEAT_CONTROLLER_INITIAL_DELAY_CONFIG = new Utils.ConfigEntity<>(
      "controller.batch.job.heartbeat.initial.delay.ms",
      Duration.ofMinutes(2).toMillis(),
      "The amount of time the controller waits after a store creation before it enables the heartbeat-based lingering push job checking feature.");
  public static final Utils.ConfigEntity<Long> HEARTBEAT_INITIAL_DELAY_CONFIG =
      new Utils.ConfigEntity<>("batch.job.heartbeat.initial.delay.ms", 0L, "Delay before sending the first heartbeat");
  public static final Utils.ConfigEntity<Boolean> HEARTBEAT_LAST_HEARTBEAT_IS_DELETE_CONFIG = new Utils.ConfigEntity<>(
      "heartbeat.is.last.heartbeat.delete",
      true,
      "Whether the last heartbeat message is a DELETE record which deletes the existing k-v pair of a BatchJobHeartbeatKey. "
          + "In production, it should be true because once a VPJ run finishes, its liveness heartbeat should be deleted "
          + "to indicate that it has finished (no longer alive). However, sometimes we may want to preserve heartbeat "
          + "for verification purpose, for example, in an integration test.");

  private BatchJobHeartbeatConfigs() {
    // Util class
  }
}
