package com.linkedin.venice.controller.lingeringjob;

import clojure.lang.MapEntry;
import com.linkedin.venice.common.VeniceSystemStoreType;
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
      Optional<X509Certificate> requesterCert,
      IdentityParser identityParser
  ) {
    if (isBatchJobHeartbeatEnabled(store, version, time, controllerAdmin, requesterCert, identityParser)) {
      LOGGER.info(String.format("Batch job heartbeat is enabled for store %s with version %d", store.getName(), version.getNumber()));
      return !isBatchJobAlive(store, version, time, controllerAdmin); // A not-alive job is lingering
    }
    LOGGER.info(String.format("Batch job heartbeat is not enabled for store %s with version %d. "
        + "Fall back to the default behavior.", store.getName(), version.getNumber()));
    return defaultLingeringStoreVersionChecker.isStoreVersionLingering(
        store,
        version,
        time,
        controllerAdmin,
        requesterCert,
        identityParser
    );
  }

  private boolean isBatchJobAlive(Store store, Version version, Time time, Admin controllerAdmin) {
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
      final long timeSinceJobStartedMs = time.getMilliseconds() - version.getCreatedTime();
      if (timeSinceJobStartedMs < initialHeartbeatBufferTime.toMillis()) {
        // Even if there is no heartbeat for this store version, we still check the initial heartbeat buffer time.
        // It is to handle the case where the initial heartbeat takes time to propagate/travel to the heartbeat store.
        // In other words, there could be no heartbeat since the first of its heartbeat is on its way to the heartbeat store.
        // So, we assume that the batch job is alive to avoid falsely killing an alive and heart beating job.
        LOGGER.info(String.format("No heartbeat found for store %s with version %d. However, still assume it is alive, "
            + "because this store version was created %d ms ago and this duration is shorter than the buffer time %d ms.",
            store.getName(), version.getNumber(), timeSinceJobStartedMs, initialHeartbeatBufferTime.toMillis()));
        return true;
      }
      LOGGER.info(String.format("No heartbeat found for store %s with version %d with created time %d ms",
          store.getName(), version.getNumber(), version.getCreatedTime()));
      return false;
    }
    final long timeSinceLastHeartbeatMs = time.getMilliseconds() - lastSeenHeartbeatValue.timestamp;
    if (timeSinceLastHeartbeatMs > heartbeatTimeout.toMillis()) {
      LOGGER.info(String.format("Heartbeat timed out for store %s with version %d. Timeout threshold is %d ms and time"
                      + " since last heartbeat in ms is: %d",
              store.getName(), version.getNumber(), heartbeatTimeout.toMillis(), timeSinceLastHeartbeatMs));
      heartbeatBasedCheckerStats.recordTimeoutHeartbeatCheck();
      return false;
    }
    LOGGER.info(String.format("Heartbeat detected for store %s with version %d and time since last heartbeat is: %d ms " +
                    "and the timeout threshold is %s ms",
        store.getName(), version.getNumber(), timeSinceLastHeartbeatMs, heartbeatTimeout.toMillis()));
    heartbeatBasedCheckerStats.recordNoTimeoutHeartbeatCheck();
    return true;
  }

  private boolean isBatchJobHeartbeatEnabled(
      Store store,
      Version version,
      Time time,
      Admin controllerAdmin,
      Optional<X509Certificate> requesterCert,
      IdentityParser identityParser
  ) {
    if (!canRequesterAccessHeartbeatStore(controllerAdmin, requesterCert, identityParser)) {
      String requestIdentity;
      if (requesterCert.isPresent()) {
        requestIdentity = identityParser.parseIdentityFromCert(requesterCert.get());
      } else {
        requestIdentity = "unknown (no cert)";
      }

      LOGGER.warn(String.format("Assume the batch job heartbeat is not enabled since it does not have write access to the "
          + "heartbeat store. Requested store %s with version %d for requester: %s",
          store.getName(),
          version.getNumber(),
          requestIdentity
      ));
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
    LOGGER.info("For store " + store.getName() + " with version " + version.getNumber() + ", found pushJobConfigs: " + pushJobConfigs);
    Optional<CharSequence> heartbeatEnableConfigValue = Utils.getValueFromCharSequenceMapWithStringKey(
            pushJobConfigs,
            HEARTBEAT_ENABLED_CONFIG.getConfigName()
    );
    if (heartbeatEnableConfigValue.isPresent()) {
      return Boolean.parseBoolean(heartbeatEnableConfigValue.get().toString());
    } else {
      return false; // No config given on this property. Assume not enabled to be safe.
    }
  }

  private boolean canRequesterAccessHeartbeatStore(
      Admin controllerAdmin,
      Optional<X509Certificate> requesterCert,
      IdentityParser identityParser
  ) {
    if (!requesterCert.isPresent()) {
      LOGGER.warn("No requester cert is provided. Hence assume the requester has no write permission to the heartbeat store");
      return false;
    }
    try {
      return controllerAdmin.hasWritePermissionToBatchJobHeartbeatStore(
          requesterCert.get(),
          VeniceSystemStoreType.BATCH_JOB_HEARTBEAT_STORE.getPrefix(),
          identityParser
      );
    } catch (Exception e) {
      LOGGER.warn("Cannot check access permission. Assume no access permission.", e);
    }
    return false;
  }
}
