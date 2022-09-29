package com.linkedin.venice.controller.datarecovery;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.venice.client.store.AvroSpecificStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.VeniceHelixAdmin;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.meta.DataRecoveryVersionConfigImpl;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.participant.protocol.ParticipantMessageKey;
import com.linkedin.venice.participant.protocol.ParticipantMessageValue;
import com.linkedin.venice.participant.protocol.enums.ParticipantMessageType;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.service.ICProvider;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.io.Closeable;
import java.util.Map;
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
  private final D2Client d2Client;
  private final String clusterDiscoveryD2ServiceName;
  private final Optional<ICProvider> icProvider;
  private final Map<String, AvroSpecificStoreClient<ParticipantMessageKey, ParticipantMessageValue>> clientMap =
      new VeniceConcurrentHashMap<>();

  public DataRecoveryManager(
      VeniceHelixAdmin veniceAdmin,
      D2Client d2Client,
      String clusterDiscoveryD2ServiceName,
      Optional<ICProvider> icProvider) {
    this.veniceAdmin = veniceAdmin;
    this.d2Client = d2Client;
    this.clusterDiscoveryD2ServiceName = clusterDiscoveryD2ServiceName;
    this.icProvider = icProvider;
  }

  /**
   * Some functionality of the data recovery manager requires ClientConfig which requires D2Client to be available.
   * Check and throw exceptions if D2Client is not provided.
   * @param feature name of the data recovery manager that requires ClientConfig for logging/informative purpose.
   */
  private void ensureClientConfigIsAvailable(String feature) {
    if (d2Client == null) {
      throw new VeniceException("DataRecoveryManger requires D2Client to " + feature + " but null is provided");
    }
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
    Version dataRecoveryVersion = sourceFabricVersion.cloneVersion();
    dataRecoveryVersion.setStatus(VersionStatus.STARTED);
    Store store = veniceAdmin.getStore(clusterName, storeName);
    if (store == null) {
      throw new VeniceNoStoreException(storeName, clusterName);
    }
    // L/F and NR are required for data recovery.
    dataRecoveryVersion.setLeaderFollowerModelEnabled(store.isLeaderFollowerModelEnabled());
    dataRecoveryVersion.setNativeReplicationEnabled(store.isNativeReplicationEnabled());
    dataRecoveryVersion.setNativeReplicationSourceFabric(sourceFabric);
    dataRecoveryVersion.setDataRecoveryVersionConfig(new DataRecoveryVersionConfigImpl(sourceFabric, false));

    dataRecoveryVersion.setUseVersionLevelHybridConfig(true);
    if (!copyAllVersionConfigs) {
      dataRecoveryVersion.setActiveActiveReplicationEnabled(store.isActiveActiveReplicationEnabled());
      dataRecoveryVersion.setReplicationFactor(store.getReplicationFactor());
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
      int versionNumber,
      int sourceAmplificationFactor) {
    verifyStoreIsCapableOfDataRecovery(clusterName, storeName, sourceAmplificationFactor);
    Store store = veniceAdmin.getStore(clusterName, storeName);
    if (store.getCurrentVersion() == versionNumber) {
      /**
       * We need to set the store's current version to the backup version or {@link Store#NON_EXISTING_VERSION} in order to
       * perform data recovery on the current version.
       */
      Optional<Version> backupVersion =
          store.getVersions().stream().filter(v -> v.getNumber() != versionNumber).findFirst();
      veniceAdmin.updateStore(
          clusterName,
          storeName,
          new UpdateStoreQueryParams()
              .setCurrentVersion(backupVersion.map(Version::getNumber).orElse(Store.NON_EXISTING_VERSION)));
    }
    veniceAdmin.wipeCluster(clusterName, destinationFabric, Optional.of(storeName), Optional.of(versionNumber));
    veniceAdmin.deleteParticipantStoreKillMessage(clusterName, Version.composeKafkaTopic(storeName, versionNumber));
  }

  private void verifyStoreIsCapableOfDataRecovery(String clusterName, String storeName, int sourceAmplificationFactor) {
    Store store = veniceAdmin.getStore(clusterName, storeName);
    if (store == null) {
      throw new VeniceNoStoreException(storeName, clusterName);
    }
    if (store.isMigrating()) {
      throw new VeniceException("Data recovery is not allowed during store migration");
    }
    if (!store.isLeaderFollowerModelEnabled()) {
      throw new VeniceException("Leader follower model is required for data recovery");
    }
    if (!store.isNativeReplicationEnabled()) {
      throw new VeniceException("Native replication is required for data recovery");
    }
    // In parent controller we have the information to check if source and destination fabric store configurations will
    // allow data recovery or not.
    if (sourceAmplificationFactor != store.getPartitionerConfig().getAmplificationFactor()) {
      throw new VeniceException("Amplification factor is not the same between source and destination fabric");
    }
  }

  /**
   * Verify that target store version is ready for data recovery.
   */
  public void verifyStoreVersionIsReadyForDataRecovery(
      String clusterName,
      String storeName,
      int versionNumber,
      int sourceAmplificationFactor) {
    verifyStoreIsCapableOfDataRecovery(clusterName, storeName, sourceAmplificationFactor);
    ensureClientConfigIsAvailable("verify store version is ready for data recovery");
    Store store = veniceAdmin.getStore(clusterName, storeName);
    if (store == null) {
      throw new VeniceNoStoreException(storeName, clusterName);
    }
    if (store.getVersion(versionNumber).isPresent()) {
      throw new VeniceException("Previous store version metadata still exists");
    }
    String kafkaTopic = Version.composeKafkaTopic(storeName, versionNumber);
    if (veniceAdmin.getTopicManager().containsTopic(kafkaTopic)) {
      throw new VeniceException("Previous version topic: " + kafkaTopic + " still exists");
    }
    if (!ExecutionStatus.NOT_CREATED
        .equals(veniceAdmin.getOffLinePushStatus(clusterName, kafkaTopic).getExecutionStatus())) {
      throw new VeniceException("Previous push status for " + kafkaTopic + " still exists");
    }
    try {
      if (!isStoreVersionKillRecordNull(clusterName, kafkaTopic)) {
        throw new VeniceException("Previous kill record for " + kafkaTopic + " still exists");
      }
    } catch (Exception e) {
      throw new VeniceException("Unable to check if the store version kill record is null", e);
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
          .call(this.getClass().getCanonicalName(), () -> getParticipantStoreClient(clusterName).get(key))
          .get();
    } else {
      value = getParticipantStoreClient(clusterName).get(key).get();
    }
    return value == null;
  }

  private AvroSpecificStoreClient<ParticipantMessageKey, ParticipantMessageValue> getParticipantStoreClient(
      String clusterName) {
    return clientMap.computeIfAbsent(clusterName, k -> {
      ClientConfig<ParticipantMessageValue> newClientConfig =
          ClientConfig
              .defaultSpecificClientConfig(
                  VeniceSystemStoreUtils.getParticipantStoreNameForCluster(clusterName),
                  ParticipantMessageValue.class)
              .setD2Client(d2Client)
              .setD2ServiceName(clusterDiscoveryD2ServiceName);
      return ClientFactory.getAndStartSpecificAvroClient(newClientConfig);
    });
  }

  /**
   * Cause all Venice avro client to close.
   */
  @Override
  public void close() {
    for (AvroSpecificStoreClient client: clientMap.values()) {
      client.close();
    }
  }
}
