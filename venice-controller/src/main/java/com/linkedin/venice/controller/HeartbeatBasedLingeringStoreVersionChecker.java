package com.linkedin.venice.controller;

import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.status.protocol.BatchJobHeartbeatKey;
import com.linkedin.venice.status.protocol.BatchJobHeartbeatValue;
import com.linkedin.venice.status.protocol.PushJobDetails;
import com.linkedin.venice.status.protocol.PushJobStatusRecordKey;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import java.time.Duration;
import java.util.Map;
import org.apache.log4j.Logger;

import static com.linkedin.venice.status.BatchJobHeartbeatConfigs.*;


public class HeartbeatBasedLingeringStoreVersionChecker implements LingeringStoreVersionChecker {
  private static final Logger LOGGER = Logger.getLogger(HeartbeatBasedLingeringStoreVersionChecker.class);

  private final Duration heartbeatTimeout;
  private final Duration initialHeartbeatBufferTime; // If a push job was started less than this much time ago, do not check heartbeat
  private final DefaultLingeringStoreVersionChecker defaultLingeringStoreVersionChecker;

  HeartbeatBasedLingeringStoreVersionChecker(
      Duration heartbeatTimeout,
      Duration initialHeartbeatBufferTime,
      DefaultLingeringStoreVersionChecker defaultLingeringStoreVersionChecker
  ) {
    this.heartbeatTimeout = Utils.notNull(heartbeatTimeout);
    this.initialHeartbeatBufferTime = Utils.notNull(initialHeartbeatBufferTime);
    this.defaultLingeringStoreVersionChecker = Utils.notNull(defaultLingeringStoreVersionChecker);
  }

  @Override
  public boolean isStoreVersionLingering(Store store, Version version, Time time, Admin controllerAdmin) {
    if (isBatchJobHeartbeatEnabled(store, version, time, controllerAdmin)) {
      LOGGER.info(String.format("Batch job heartbeat is enabled for store %s with version %d", store.getName(), version.getNumber()));
      return batchJobHasHeartbeat(store, version, time, controllerAdmin);
    }
    return defaultLingeringStoreVersionChecker.isStoreVersionLingering(store, version, time, controllerAdmin);
  }

  private boolean batchJobHasHeartbeat(Store store, Version version, Time time, Admin controllerAdmin) {
    BatchJobHeartbeatKey batchJobHeartbeatKey = new BatchJobHeartbeatKey();
    batchJobHeartbeatKey.storeName = store.getName();
    batchJobHeartbeatKey.storeVersion = version.getNumber();
    BatchJobHeartbeatValue lastSeenHeartbeatValue = controllerAdmin.getBatchJobHeartbeatValue(batchJobHeartbeatKey);
    if (lastSeenHeartbeatValue == null) {
      return false;
    }
    final long timeSinceLastHeartbeatMs = time.getMilliseconds() - lastSeenHeartbeatValue.timestamp;
    if (timeSinceLastHeartbeatMs > heartbeatTimeout.toMillis()) {
      LOGGER.info(String.format("Heartbeat timed out for store %s with version %d. Timeout is %s and time" + " since last heartbeat in ms is: %d", store.getName(), version.getNumber(), heartbeatTimeout,
          timeSinceLastHeartbeatMs));
      return false;
    }
    LOGGER.info(String.format("Heartbeat detected for store %s with version %d and time since last heartbeat in ms " + "is: %d",
        store.getName(), version.getNumber(), timeSinceLastHeartbeatMs));
    return true;
  }

  private boolean isBatchJobHeartbeatEnabled(Store store, Version version, Time time, Admin controllerAdmin) {
    PushJobStatusRecordKey pushJobStatusRecordKey = new PushJobStatusRecordKey();
    pushJobStatusRecordKey.storeName = store.getName();
    pushJobStatusRecordKey.versionNumber = version.getNumber();
    PushJobDetails pushJobDetails;
    try {
      pushJobDetails = controllerAdmin.getPushJobDetails(pushJobStatusRecordKey);
    } catch (Exception e) {
      LOGGER.error("Cannot determine if batch job heartbeat is enabled or not with exception. Assume it is "
          + "not enabled", e);
      return false;
    }
    if (pushJobDetails == null) {
      LOGGER.warn(String.format("Found no push job details for store %s version %d", store.getName(), version.getNumber()));
      return false;
    }
    Map<CharSequence, CharSequence> pushJobConfigs = pushJobDetails.pushJobConfigs;
    if (pushJobConfigs == null) {
      LOGGER.warn("Null push job configs in the push job details event");
      return false;
    }
    boolean heartbeatEnabledInConfig = Boolean.parseBoolean(
        (String) pushJobConfigs.getOrDefault(HEARTBEAT_ENABLED_CONFIG.getConfigName(), "false"));
    if (!heartbeatEnabledInConfig) {
      return false;
    }
    // Even if config says heartbeat is enabled, we still check the initial heartbeat buffer time. It is to handle
    // the case where the initial heartbeat takes time to propagate/travel to the heartbeat store
    final long timeSinceJobStartedMs = time.getMilliseconds() - version.getCreatedTime();
    return timeSinceJobStartedMs > initialHeartbeatBufferTime.toMillis();
  }
}