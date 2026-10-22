package com.linkedin.venice.controller.init;

import static com.linkedin.venice.ConfigKeys.CLUSTER_PUSH_STATUS_STORE_ENABLED;
import static com.linkedin.venice.common.VeniceSystemStoreUtils.CLUSTER_PUSH_STATUS_STORE_PARTITION_COUNT;

import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.VeniceControllerMultiClusterConfig;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.manager.TopicManager;
import com.linkedin.venice.pushstatus.PushStatusKey;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.schema.writecompute.DerivedSchemaEntry;
import com.linkedin.venice.schema.writecompute.WriteComputeSchemaConverter;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.utils.Utils;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;


public class ClusterPushStatusStoreInitializationRoutine implements ClusterLeaderInitializationRoutine {
  private final VeniceControllerMultiClusterConfig configs;
  private final Admin admin;

  public ClusterPushStatusStoreInitializationRoutine(VeniceControllerMultiClusterConfig configs, Admin admin) {
    this.configs = configs;
    this.admin = admin;
  }

  @Override
  public void execute(String clusterName) {
    if (!configs.getControllerConfig(clusterName).getProps().getBoolean(CLUSTER_PUSH_STATUS_STORE_ENABLED, false)) {
      return;
    }
    String storeName = VeniceSystemStoreUtils.getDaVinciPushStatusStoreNameForCluster(clusterName);
    validateExistingStore(clusterName, storeName);
    SystemStoreInitializationHelper.setupSystemStore(
        clusterName,
        storeName,
        AvroProtocolDefinition.PUSH_STATUS_SYSTEM_SCHEMA_STORE,
        PushStatusKey.SCHEMA$,
        store -> !store.isHybrid() || !store.isWriteComputationEnabled()
            || store.getPartitionCount() != CLUSTER_PUSH_STATUS_STORE_PARTITION_COUNT,
        new UpdateStoreQueryParams().setPartitionCount(CLUSTER_PUSH_STATUS_STORE_PARTITION_COUNT)
            .setHybridOffsetLagThreshold(1)
            .setHybridRewindSeconds(TimeUnit.DAYS.toSeconds(1))
            .setHybridTimeLagThreshold(-1)
            .setWriteComputationEnabled(true)
            .setNativeReplicationSourceFabric(configs.getRegionName()),
        admin,
        configs);
    Collection<DerivedSchemaEntry> derivedSchemas = admin.getDerivedSchemas(clusterName, storeName);
    for (SchemaEntry valueSchema: admin.getValueSchemas(clusterName, storeName)) {
      Schema updateSchema =
          WriteComputeSchemaConverter.getInstance().convertFromValueRecordSchema(valueSchema.getSchema());
      DerivedSchemaEntry existing = derivedSchemas.stream()
          .filter(entry -> entry.getValueSchemaID() == valueSchema.getId() && entry.getId() == 1)
          .findFirst()
          .orElse(null);
      if (existing == null) {
        admin.addDerivedSchema(clusterName, storeName, valueSchema.getId(), 1, updateSchema.toString());
      } else if (!existing.getSchema().equals(updateSchema)) {
        throw new VeniceException("Inconsistent push status update schema in " + storeName);
      }
    }
  }

  private void validateExistingStore(String clusterName, String storeName) {
    Store store = admin.getStore(clusterName, storeName);
    boolean requiresRealTimeTopic = false;
    if (store != null) {
      if (store.isHybrid() && store.getPartitionCount() != CLUSTER_PUSH_STATUS_STORE_PARTITION_COUNT) {
        throw new VeniceException(
            "Incompatible push status store " + storeName + ": hybrid store must have "
                + CLUSTER_PUSH_STATUS_STORE_PARTITION_COUNT + " partitions");
      }
      int currentVersion = store.getCurrentVersion();
      if (currentVersion != Store.NON_EXISTING_VERSION && store.getVersion(currentVersion) == null) {
        throw new VeniceException(
            "Incompatible push status store " + storeName + ": missing current version " + currentVersion);
      }
      for (Version version: store.getVersions()) {
        if (version.getNumber() == currentVersion || version.getStatus() == VersionStatus.ONLINE) {
          if (version.getPartitionCount() != CLUSTER_PUSH_STATUS_STORE_PARTITION_COUNT || !version.isHybrid()) {
            throw new VeniceException(
                "Incompatible push status store " + storeName + ": serving version " + version.getNumber()
                    + " must be hybrid with " + CLUSTER_PUSH_STATUS_STORE_PARTITION_COUNT + " partitions");
          }
          if (!store.isWriteComputationEnabled()) {
            throw new VeniceException(
                "Incompatible push status store " + storeName + ": serving version " + version.getNumber()
                    + " requires write computation to be enabled before ingestion starts");
          }
          requiresRealTimeTopic = true;
        }
      }
    }
    PubSubTopic realTimeTopic = admin.getPubSubTopicRepository()
        .getTopic(store == null ? Utils.composeRealTimeTopic(storeName) : Utils.getRealTimeTopicName(store));
    TopicManager topicManager = admin.getTopicManager();
    if (topicManager.containsTopic(realTimeTopic)) {
      if (topicManager.getPartitionCount(realTimeTopic) != CLUSTER_PUSH_STATUS_STORE_PARTITION_COUNT) {
        throw new VeniceException(
            "Incompatible push status RT topic " + realTimeTopic.getName() + ": must have "
                + CLUSTER_PUSH_STATUS_STORE_PARTITION_COUNT + " partitions");
      }
    } else if (requiresRealTimeTopic) {
      throw new VeniceException("Missing push status RT topic " + realTimeTopic.getName() + " for serving version");
    }
  }
}
