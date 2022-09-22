package com.linkedin.venice.controller.lingeringjob;

import static com.linkedin.venice.status.BatchJobHeartbeatConfigs.HEARTBEAT_ENABLED_CONFIG;

import com.linkedin.venice.authorization.IdentityParser;
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
import javax.annotation.Nonnull;
import org.apache.commons.lang.Validate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class HeartbeatBasedLingeringStoreVersionChecker implements LingeringStoreVersionChecker {
  private static final Logger LOGGER = LogManager.getLogger(HeartbeatBasedLingeringStoreVersionChecker.class);
  private final Duration heartbeatTimeout;
  private final Duration initialHeartbeatBufferTime; // If a push job was started less than this much time ago, do not
                                                     // check heartbeat
  private final DefaultLingeringStoreVersionChecker defaultLingeringStoreVersionChecker;
  private final HeartbeatBasedCheckerStats heartbeatBasedCheckerStats;

  public HeartbeatBasedLingeringStoreVersionChecker(
      @Nonnull Duration heartbeatTimeout,
      @Nonnull Duration initialHeartbeatBufferTime,
      @Nonnull DefaultLingeringStoreVersionChecker defaultLingeringStoreVersionChecker,
      @Nonnull HeartbeatBasedCheckerStats heartbeatBasedCheckerStats) {
    Validate.notNull(heartbeatTimeout);
    Validate.notNull(initialHeartbeatBufferTime);
    Validate.notNull(defaultLingeringStoreVersionChecker);
    Validate.notNull(heartbeatBasedCheckerStats);
    this.heartbeatTimeout = heartbeatTimeout;
    this.initialHeartbeatBufferTime = initialHeartbeatBufferTime;
    this.defaultLingeringStoreVersionChecker = defaultLingeringStoreVersionChecker;
    this.heartbeatBasedCheckerStats = heartbeatBasedCheckerStats;
    LOGGER.info(
        "Instance is created with [initialHeartbeatBufferTime={}] and [heartbeatTimeout={}]",
        initialHeartbeatBufferTime,
        heartbeatTimeout);
  }

  @Override
  public boolean isStoreVersionLingering(
      Store store,
      Version version,
      Time time,
      Admin controllerAdmin,
      Optional<X509Certificate> requesterCert,
      IdentityParser identityParser) {
    if (isBatchJobHeartbeatEnabled(store, version, controllerAdmin, requesterCert, identityParser)) {
      LOGGER.info("Batch job heartbeat is enabled for store {} with version {}", store.getName(), version.getNumber());
      return !isBatchJobAlive(store, version, time, controllerAdmin); // A not-alive job is lingering
    }
    LOGGER.info(
        "Batch job heartbeat is not enabled for store {} with version {}. Fall back to the default behavior.",
        store.getName(),
        version.getNumber());
    return defaultLingeringStoreVersionChecker
        .isStoreVersionLingering(store, version, time, controllerAdmin, requesterCert, identityParser);
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
      LOGGER.warn(
          "Got exception when getting heartbeat value. Store {} with version {}",
          store.getName(),
          version.getNumber(),
          e);
      heartbeatBasedCheckerStats.recordCheckJobHasHeartbeatFailed();
      return true; // Assume the job has heartbeat to avoid falsely terminating a running job
    }
    if (lastSeenHeartbeatValue == null) {
      final long timeSinceJobStartedMs = time.getMilliseconds() - version.getCreatedTime();
      if (timeSinceJobStartedMs < initialHeartbeatBufferTime.toMillis()) {
        // Even if there is no heartbeat for this store version, we still check the initial heartbeat buffer time.
        // It is to handle the case where the initial heartbeat takes time to propagate/travel to the heartbeat store.
        // In other words, there could be no heartbeat since the first of its heartbeat is on its way to the heartbeat
        // store.
        // So, we assume that the batch job is alive to avoid falsely killing an alive and heart beating job.
        LOGGER.info(
            "No heartbeat found for store {} with version {}. However, still assume it is alive, "
                + "because this store version was created {} ms ago and this duration is shorter than the buffer time {} ms.",
            store.getName(),
            version.getNumber(),
            timeSinceJobStartedMs,
            initialHeartbeatBufferTime.toMillis());
        return true;
      }
      LOGGER.info(
          "No heartbeat found for store {} with version {} with created time {} ms",
          store.getName(),
          version.getNumber(),
          version.getCreatedTime());
      return false;
    }
    final long timeSinceLastHeartbeatMs = time.getMilliseconds() - lastSeenHeartbeatValue.timestamp;
    if (timeSinceLastHeartbeatMs > heartbeatTimeout.toMillis()) {
      LOGGER.info(
          "Heartbeat timed out for store {} with version {}. Timeout threshold "
              + "is {} ms and time since last heartbeat is: {} ms",
          store.getName(),
          version.getNumber(),
          heartbeatTimeout.toMillis(),
          timeSinceLastHeartbeatMs);
      heartbeatBasedCheckerStats.recordTimeoutHeartbeatCheck();
      return false;
    }
    LOGGER.info(
        "Heartbeat detected for store {} with version {} and time "
            + "since last heartbeat is: {} ms and the timeout threshold is {} ms",
        store.getName(),
        version.getNumber(),
        timeSinceLastHeartbeatMs,
        heartbeatTimeout.toMillis());
    heartbeatBasedCheckerStats.recordNoTimeoutHeartbeatCheck();
    return true;
  }

  private boolean isBatchJobHeartbeatEnabled(
      Store store,
      Version version,
      Admin controllerAdmin,
      Optional<X509Certificate> requesterCert,
      IdentityParser identityParser) {
    if (!canRequesterAccessHeartbeatStore(controllerAdmin, requesterCert)) {
      String requestIdentity;
      if (requesterCert.isPresent()) {
        requestIdentity = identityParser.parseIdentityFromCert(requesterCert.get());
      } else {
        requestIdentity = "unknown (no cert)";
      }

      LOGGER.warn(
          "Assume the batch job heartbeat is not enabled since it does not have write access to the "
              + "heartbeat store. Requested store {} with version {} for requester: {}",
          store.getName(),
          version.getNumber(),
          requestIdentity);
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
      LOGGER.error(
          "Cannot determine if batch job heartbeat is enabled or not with exception. "
              + "Assume it is not enabled. Store {} and its version {}",
          store.getName(),
          version.getNumber(),
          e);
      heartbeatBasedCheckerStats.recordCheckJobHasHeartbeatFailed();
      return false;
    }
    if (pushJobDetails == null) {
      LOGGER.warn("Found no push job details for store {} version {}", store.getName(), version.getNumber());
      return false;
    }
    Map<CharSequence, CharSequence> pushJobConfigs = pushJobDetails.pushJobConfigs;
    if (pushJobConfigs == null) {
      LOGGER.warn(
          "Null push job configs in the push job details event. Store {} and its version {}",
          store.getName(),
          version.getNumber());
      heartbeatBasedCheckerStats.recordCheckJobHasHeartbeatFailed();
      return false;
    }
    LOGGER.info(
        "For store {} with version {}, found pushJobConfigs: {}",
        store.getName(),
        version.getNumber(),
        pushJobConfigs);
    Optional<CharSequence> heartbeatEnableConfigValue =
        Utils.getValueFromCharSequenceMapWithStringKey(pushJobConfigs, HEARTBEAT_ENABLED_CONFIG.getConfigName());
    if (heartbeatEnableConfigValue.isPresent()) {
      return Boolean.parseBoolean(heartbeatEnableConfigValue.get().toString());
    } else {
      return false; // No config given on this property. Assume not enabled to be safe.
    }
  }

  private boolean canRequesterAccessHeartbeatStore(Admin controllerAdmin, Optional<X509Certificate> requesterCert) {
    if (!requesterCert.isPresent()) {
      LOGGER.warn(
          "No requester cert is provided. Hence assume the requester has no write permission to the heartbeat store");
      return false;
    }
    try {
      return controllerAdmin.hasWritePermissionToBatchJobHeartbeatStore(
          requesterCert.get(),
          VeniceSystemStoreType.BATCH_JOB_HEARTBEAT_STORE.getPrefix());
    } catch (Exception e) {
      LOGGER.warn("Cannot check access permission. Assume no access permission.", e);
    }
    return false;
  }
}
