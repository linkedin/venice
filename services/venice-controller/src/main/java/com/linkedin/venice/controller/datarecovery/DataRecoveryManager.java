package com.linkedin.venice.controller.datarecovery;

import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.ParticipantStoreClientsManager;
import com.linkedin.venice.controller.VeniceHelixAdmin;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.meta.DataRecoveryVersionConfigImpl;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.participant.protocol.ParticipantMessageKey;
import com.linkedin.venice.participant.protocol.ParticipantMessageValue;
import com.linkedin.venice.participant.protocol.enums.ParticipantMessageType;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.service.ICProvider;
import java.io.Closeable;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Optional;


/**
 * This class contains all the logics to:
 *   1. Validate a Venice store version for data recovery. i.e. make sure the destination fabric is capable of data
 *   recovery and its previous sates are reset/cleared properly.
 *   2. Prepare a Venice store version for data recovery. i.e. delete the existing version, Helix resources and kafka
 *   topic.
 *   3. Initiate the data recovery by recreating the version, kafka topic and Helix resources accordingly.
 */
public class DataRecoveryManager implements Closeable {
  private final VeniceHelixAdmin veniceAdmin;
  private final Optional<ICProvider> icProvider;
  private final PubSubTopicRepository pubSubTopicRepository;
  private ParticipantStoreClientsManager participantStoreClientsManager;

  public DataRecoveryManager(
      VeniceHelixAdmin veniceAdmin,
      Optional<ICProvider> icProvider,
      PubSubTopicRepository pubSubTopicRepository,
      ParticipantStoreClientsManager participantStoreClientsManager) {
    this.veniceAdmin = veniceAdmin;
    this.icProvider = icProvider;
    this.pubSubTopicRepository = pubSubTopicRepository;
    this.participantStoreClientsManager = participantStoreClientsManager;
  }

  private String getRecoveryPushJobId(String srcPushJobId) {
    final String prefix = "data-recovery";
    if (!srcPushJobId.startsWith(prefix)) {
      return String.format("%s(%s)_%s", prefix, LocalDateTime.now(ZoneOffset.UTC), srcPushJobId);
    }
    return srcPushJobId
        .replaceFirst("data-recovery\\(.*\\)", String.format("%s(%s)", prefix, LocalDateTime.now(ZoneOffset.UTC)));
  }

  /**
   * Initiate data recovery process by recreating the version, kafka topic, and Helix resources accordingly.
   */
  public void initiateDataRecovery(
      String clusterName,
      String storeName,
      int version,
      String sourceFabric,
      boolean copyAllVersionConfigs,
      Version sourceFabricVersion) {
    Store store = veniceAdmin.getStore(clusterName, storeName);
    if (store == null) {
      throw new VeniceNoStoreException(storeName, clusterName);
    }
    int srcFabricVersionNumber = sourceFabricVersion.getNumber();
    if (srcFabricVersionNumber != version) {
      sourceFabricVersion.setNumber(version);
      /**
       * Update the push job id as a version with same id cannot be added twice.
       * @see VeniceHelixAdmin#addSpecificVersion(String, String, Version)
       */
      sourceFabricVersion.setPushJobId(getRecoveryPushJobId(sourceFabricVersion.getPushJobId()));
    }
    Version dataRecoveryVersion = sourceFabricVersion.cloneVersion();
    dataRecoveryVersion.setStatus(VersionStatus.STARTED);
    dataRecoveryVersion
        .setDataRecoveryVersionConfig(new DataRecoveryVersionConfigImpl(sourceFabric, false, srcFabricVersionNumber));

    dataRecoveryVersion.setUseVersionLevelIncrementalPushEnabled(true);
    dataRecoveryVersion.setUseVersionLevelHybridConfig(true);
    if (!copyAllVersionConfigs) {
      dataRecoveryVersion.setActiveActiveReplicationEnabled(store.isActiveActiveReplicationEnabled());
      dataRecoveryVersion.setReplicationFactor(store.getReplicationFactor());
      dataRecoveryVersion.setIncrementalPushEnabled(store.isIncrementalPushEnabled());
    }
    boolean versionAdded = veniceAdmin.addSpecificVersion(clusterName, storeName, dataRecoveryVersion);
    if (!versionAdded) {
      throw new VeniceException(
          "Failed to add version: " + version + " to store: " + storeName
              + " because another version with the push id already exist. Push id: "
              + dataRecoveryVersion.getPushJobId());
    }
    veniceAdmin.createSpecificVersionTopic(clusterName, storeName, dataRecoveryVersion);
    veniceAdmin.createHelixResourceAndStartMonitoring(clusterName, storeName, dataRecoveryVersion);
  }

  /**
   * @see Admin#prepareDataRecovery(String, String, int, String, String, Optional)
   */
  public void prepareStoreVersionForDataRecovery(
      String clusterName,
      String storeName,
      String destinationFabric,
      int versionNumber) {
    verifyStoreIsCapableOfDataRecovery(clusterName, storeName);
    Store store = veniceAdmin.getStore(clusterName, storeName);
    String topic = Version.composeKafkaTopic(storeName, versionNumber);
    if (store.getCurrentVersion() == versionNumber) {
      if (!veniceAdmin.isClusterWipeAllowed(clusterName)) {
        throw new VeniceException(destinationFabric + " cluster " + clusterName + " cannot wipe current version");
      }
      /**
       * We need to set the store's current version to the backup version or {@link Store#NON_EXISTING_VERSION} to
       * perform data recovery on the current version.
       */
      int backupVersion = veniceAdmin.getBackupVersionNumber(store.getVersions(), store.getCurrentVersion());
      veniceAdmin.setStoreCurrentVersion(clusterName, storeName, backupVersion);
      veniceAdmin.wipeCluster(clusterName, destinationFabric, Optional.of(storeName), Optional.of(versionNumber));
    } else {
      veniceAdmin.deleteOneStoreVersion(clusterName, storeName, versionNumber);
      veniceAdmin.stopMonitorOfflinePush(clusterName, topic, true, true);
    }
    veniceAdmin.deleteParticipantStoreKillMessage(clusterName, topic);
  }

  private void verifyStoreIsCapableOfDataRecovery(String clusterName, String storeName) {
    Store store = veniceAdmin.getStore(clusterName, storeName);
    if (store == null) {
      throw new VeniceNoStoreException(storeName, clusterName);
    }
    if (store.isMigrating()) {
      throw new VeniceException("Data recovery is not allowed during store migration");
    }
    if (!store.isNativeReplicationEnabled()) {
      throw new VeniceException("Native replication is required for data recovery");
    }
  }

  /**
   * Verify that target store version is ready for data recovery.
   */
  public void verifyStoreVersionIsReadyForDataRecovery(String clusterName, String storeName, int versionNumber) {
    verifyStoreIsCapableOfDataRecovery(clusterName, storeName);
    Store store = veniceAdmin.getStore(clusterName, storeName);
    if (store == null) {
      throw new VeniceNoStoreException(storeName, clusterName);
    }
    if (store.getVersion(versionNumber) != null) {
      throw new VeniceException("Previous store version metadata still exists");
    }
    PubSubTopic versionTopic = pubSubTopicRepository.getTopic(Version.composeKafkaTopic(storeName, versionNumber));
    if (veniceAdmin.getTopicManager().containsTopic(versionTopic)) {
      throw new VeniceException("Previous version topic: " + versionTopic + " still exists");
    }
    if (!ExecutionStatus.NOT_CREATED
        .equals(veniceAdmin.getOffLinePushStatus(clusterName, versionTopic.getName()).getExecutionStatus())) {
      throw new VeniceException("Previous push status for " + versionTopic + " still exists");
    }
    try {
      if (!isStoreVersionKillRecordNull(clusterName, versionTopic.getName())) {
        throw new VeniceException("Previous kill record for " + versionTopic + " still exists");
      }
    } catch (Exception e) {
      throw new VeniceException(e);
    }
  }

  /**
   * Check the participant store to see if there are any existing kill record for the kafka topic of interest.
   */
  private boolean isStoreVersionKillRecordNull(String clusterName, String kafkaTopic) throws Exception {
    ParticipantMessageKey key = new ParticipantMessageKey();
    key.messageType = ParticipantMessageType.KILL_PUSH_JOB.getValue();
    key.resourceName = kafkaTopic;
    ParticipantMessageValue value;
    if (icProvider.isPresent()) {
      value = icProvider.get()
          .call(
              this.getClass().getCanonicalName(),
              () -> participantStoreClientsManager.getReader(clusterName).get(key))
          .get();
    } else {
      value = participantStoreClientsManager.getReader(clusterName).get(key).get();
    }
    return value == null;
  }

  @Override
  public void close() {
  }
}
