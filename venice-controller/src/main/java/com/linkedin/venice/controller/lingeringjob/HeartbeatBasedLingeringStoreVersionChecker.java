package com.linkedin.venice.controller.lingeringjob;

import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.status.protocol.BatchJobHeartbeatKey;
import com.linkedin.venice.status.protocol.BatchJobHeartbeatValue;
import com.linkedin.venice.status.protocol.PushJobDetails;
import com.linkedin.venice.status.protocol.PushJobStatusRecordKey;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import org.apache.log4j.Logger;

import static com.linkedin.venice.status.BatchJobHeartbeatConfigs.*;


public class HeartbeatBasedLingeringStoreVersionChecker implements LingeringStoreVersionChecker {
  private static final Logger LOGGER = Logger.getLogger(HeartbeatBasedLingeringStoreVersionChecker.class);
  private final Duration heartbeatTimeout;
  private final Duration initialHeartbeatBufferTime; // If a push job was started less than this much time ago, do not check heartbeat
  private final DefaultLingeringStoreVersionChecker defaultLingeringStoreVersionChecker;
  private final HeartbeatBasedCheckerStats heartbeatBasedCheckerStats;

  public HeartbeatBasedLingeringStoreVersionChecker(Duration heartbeatTimeout, Duration initialHeartbeatBufferTime,
      DefaultLingeringStoreVersionChecker defaultLingeringStoreVersionChecker, HeartbeatBasedCheckerStats heartbeatBasedCheckerStats) {
    this.heartbeatTimeout = Utils.notNull(heartbeatTimeout);
    this.initialHeartbeatBufferTime = Utils.notNull(initialHeartbeatBufferTime);
    this.defaultLingeringStoreVersionChecker = Utils.notNull(defaultLingeringStoreVersionChecker);
    this.heartbeatBasedCheckerStats = Utils.notNull(heartbeatBasedCheckerStats);
    LOGGER.info(String.format("HeartbeatBasedLingeringStoreVersionChecker instance is created with "
        + "[initialHeartbeatBufferTime=%s] and [heartbeatTimeout=%s]", initialHeartbeatBufferTime, heartbeatTimeout));
  }

  @Override
  public boolean isStoreVersionLingering(
      Store store,
      Version version,
      Time time,
      Admin controllerAdmin,
      Optional<X509Certificate> requesterCert
  ) {
    if (isBatchJobHeartbeatEnabled(store, version, time, controllerAdmin, requesterCert)) {
      LOGGER.info(String.format("Batch job heartbeat is enabled for store %s with version %d", store.getName(), version.getNumber()));
      return batchJobHasHeartbeat(store, version, time, controllerAdmin);
    }
    LOGGER.info(String.format("Batch job heartbeat is not enabled for store %s with version %d. "
        + "Fall back to the default behavior.", store.getName(), version.getNumber()));
    return defaultLingeringStoreVersionChecker.isStoreVersionLingering(
        store,
        version,
        time,
        controllerAdmin,
        requesterCert
    );
  }

  private boolean batchJobHasHeartbeat(Store store, Version version, Time time, Admin controllerAdmin) {
    BatchJobHeartbeatKey batchJobHeartbeatKey = new BatchJobHeartbeatKey();
    batchJobHeartbeatKey.storeName = store.getName();
    batchJobHeartbeatKey.storeVersion = version.getNumber();
    BatchJobHeartbeatValue lastSeenHeartbeatValue;
    try {
      lastSeenHeartbeatValue = controllerAdmin.getBatchJobHeartbeatValue(batchJobHeartbeatKey);
    } catch (Exception e) {
      // store %s with version %s for requester: %s", store.getName(), version.getNumber()
      LOGGER.warn(String.format("Got exception when getting heartbeat value. Store %s with version %s",
          store.getName(), version.getNumber()), e);
      heartbeatBasedCheckerStats.recordCheckJobHasHeartbeatFailed();
      return true; // Assume the job has heartbeat to avoid falsely terminating a running job
    }
    if (lastSeenHeartbeatValue == null) {
      return false;
    }
    final long timeSinceLastHeartbeatMs = time.getMilliseconds() - lastSeenHeartbeatValue.timestamp;
    if (timeSinceLastHeartbeatMs > heartbeatTimeout.toMillis()) {
      LOGGER.info(String.format("Heartbeat timed out for store %s with version %d. Timeout is %s and time" + " since last heartbeat in ms is: %d", store.getName(), version.getNumber(), heartbeatTimeout,
          timeSinceLastHeartbeatMs));
      heartbeatBasedCheckerStats.recordTimeoutHeartbeatCheck();
      return false;
    }
    LOGGER.info(String.format("Heartbeat detected for store %s with version %d and time since last heartbeat in ms " + "is: %d",
        store.getName(), version.getNumber(), timeSinceLastHeartbeatMs));
    heartbeatBasedCheckerStats.recordNoTimeoutHeartbeatCheck();
    return true;
  }

  private boolean isBatchJobHeartbeatEnabled(
      Store store,
      Version version,
      Time time,
      Admin controllerAdmin,
      Optional<X509Certificate> requesterCert
  ) {
    if (!canRequesterAccessHeartbeatStore(controllerAdmin, requesterCert)) {
      LOGGER.warn(String.format("Assume the batch job heartbeat is not enabled since it does not have write access to the "
          + "heartbeat store %s with version %s for requester: %s", store.getName(), version.getNumber(), requesterCert.orElse(null)));
      heartbeatBasedCheckerStats.recordCheckJobHasHeartbeatFailed();
      return false;
    }
    PushJobStatusRecordKey pushJobStatusRecordKey = new PushJobStatusRecordKey();
    pushJobStatusRecordKey.storeName = store.getName();
    pushJobStatusRecordKey.versionNumber = version.getNumber();
    PushJobDetails pushJobDetails;
    try {
      pushJobDetails = controllerAdmin.getPushJobDetails(pushJobStatusRecordKey);
    } catch (Exception e) {
      LOGGER.error(String.format("Cannot determine if batch job heartbeat is enabled or not with exception. Assume it is "
          + "not enabled. Store %s and its version %s", store.getName(), version.getNumber()), e);
      heartbeatBasedCheckerStats.recordCheckJobHasHeartbeatFailed();
      return false;
    }
    if (pushJobDetails == null) {
      LOGGER.warn(String.format("Found no push job details for store %s version %d", store.getName(), version.getNumber()));
      return false;
    }
    Map<CharSequence, CharSequence> pushJobConfigs = pushJobDetails.pushJobConfigs;
    if (pushJobConfigs == null) {
      LOGGER.warn(String.format("Null push job configs in the push job details event. Store %s and its version %s",
          store.getName(), version.getNumber()));
      heartbeatBasedCheckerStats.recordCheckJobHasHeartbeatFailed();
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
    return timeSinceJobStartedMs >= initialHeartbeatBufferTime.toMillis();
  }

  private boolean canRequesterAccessHeartbeatStore(
      Admin controllerAdmin,
      Optional<X509Certificate> requesterCert
  ) {
    if (!requesterCert.isPresent()) {
      LOGGER.warn("No requester cert is provided. Hence assume the requester has no write permission to the heartbeat store");
      return false;
    }
    try {
      return controllerAdmin.hasWritePermissionToBatchJobHeartbeatStore(requesterCert.get());
    } catch (Exception e) {
      LOGGER.warn("Cannot check access permission. Assume no access permission.", e);
    }
    return false;
  }
}
