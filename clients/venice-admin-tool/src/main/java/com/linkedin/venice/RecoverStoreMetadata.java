package com.linkedin.venice;

import com.linkedin.venice.controller.kafka.AdminTopicUtils;
import com.linkedin.venice.controller.kafka.protocol.admin.AdminOperation;
import com.linkedin.venice.controller.kafka.protocol.admin.StoreCreation;
import com.linkedin.venice.controller.kafka.protocol.admin.ValueSchemaCreation;
import com.linkedin.venice.controller.kafka.protocol.enums.AdminMessageType;
import com.linkedin.venice.controller.kafka.protocol.serializer.AdminOperationSerializer;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerClientFactory;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.D2ServiceDiscoveryResponse;
import com.linkedin.venice.controllerapi.NewStoreResponse;
import com.linkedin.venice.controllerapi.SchemaResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.HelixAdapterSerializer;
import com.linkedin.venice.helix.HelixStoreGraveyard;
import com.linkedin.venice.helix.StoreJSONSerializer;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.DefaultPubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.utils.Utils;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import org.apache.helix.zookeeper.impl.client.ZkClient;


public class RecoverStoreMetadata {
  public static void recover(
      ZkClient zkClient,
      PubSubConsumerAdapter consumer,
      Optional<SSLFactory> sslFactory,
      String url,
      String storeName,
      boolean skipLastStoreCreation,
      boolean doRepair,
      List<String> graveyardClusterList,
      String recoverCluster) throws IOException {

    /**
     * Try to see whether the store exists or not.
     */
    D2ServiceDiscoveryResponse discoveryResponse = ControllerClient.discoverCluster(url, storeName, sslFactory, 3);
    if (discoveryResponse.getCluster() != null) {
      throw new VeniceException(
          "Store: " + storeName + " exists in cluster: " + discoveryResponse.getCluster() + ", no need to recover");
    }

    Map<String, List<String>> deletedStoreClusterMapping = new HashMap<>();

    HelixStoreGraveyard helixStoreGraveyard =
        new HelixStoreGraveyard(zkClient, new HelixAdapterSerializer(), graveyardClusterList);
    for (String clusterName: graveyardClusterList) {
      List<String> deletedStoreNames = helixStoreGraveyard.listStoreNamesFromGraveyard(clusterName);
      deletedStoreNames
          .forEach(s -> deletedStoreClusterMapping.computeIfAbsent(s, (ignored) -> new ArrayList()).add(clusterName));
    }
    if (!deletedStoreClusterMapping.containsKey(storeName)) {
      throw new VeniceException("Can't find store: " + storeName + " in store graveyard, so can't repair");
    }
    List<String> clusters = deletedStoreClusterMapping.get(storeName);
    if (clusters.size() > 1 && !clusters.contains(recoverCluster)) {
      throw new VeniceException("Deleted store has showed up in more one clusters: " + clusters);
    }
    if (clusters.size() == 1) {
      recoverCluster = clusters.get(0);
    }
    // Get the previous snapshot of the deleted store.
    Store deletedStore = helixStoreGraveyard.getStoreFromGraveyard(recoverCluster, storeName, null);
    if (deletedStore == null) {
      throw new VeniceException("Can't find store: " + storeName + " in the graveyard of cluster: " + recoverCluster);
    }

    // Extract the schemas from admin channel.
    String adminTopic = AdminTopicUtils.getTopicNameFromClusterName(recoverCluster);
    PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();
    PubSubTopicPartition adminTopicPartition = new PubSubTopicPartitionImpl(
        pubSubTopicRepository.getTopic(adminTopic),
        AdminTopicUtils.ADMIN_TOPIC_PARTITION_ID);
    consumer.subscribe(adminTopicPartition, -1);
    AdminOperationSerializer deserializer = new AdminOperationSerializer();
    KafkaMessageEnvelope messageEnvelope = null;

    String previousKeySchema = null;
    Map<Integer, String> previousValueSchemas = null;

    String keySchema = null;
    Map<Integer, String> valueSchemas = null;
    while (true) {
      Map<PubSubTopicPartition, List<DefaultPubSubMessage>> records = consumer.poll(3000); // 3 seconds
      if (records.isEmpty()) {
        break;
      }

      Iterator<DefaultPubSubMessage> recordsIterator = Utils.iterateOnMapOfLists(records);

      while (recordsIterator.hasNext()) {
        DefaultPubSubMessage record = recordsIterator.next();
        if (record.getPosition().getNumericOffset() % 1000 == 0) {
          System.out.println("Consumed " + record.getPosition() + " messages");
        }
        messageEnvelope = record.getValue();
        // check message type
        MessageType messageType = MessageType.valueOf(messageEnvelope);
        if (messageType.equals(MessageType.PUT)) {
          Put put = (Put) messageEnvelope.payloadUnion;
          AdminOperation adminMessage = deserializer.deserialize(put.putValue, put.schemaId);
          AdminMessageType adminMessageType = AdminMessageType.valueOf(adminMessage);
          if (AdminMessageType.STORE_CREATION == adminMessageType) {
            StoreCreation storeCreation = (StoreCreation) adminMessage.payloadUnion;
            if (storeCreation.storeName.toString().equals(storeName)) {
              /**
               * We should reset key/value schemas since the store can be created/deleted multiple times.
               */
              System.out.println(
                  "Got store creation message for store: " + storeName + " with value schema:\n"
                      + storeCreation.valueSchema.definition.toString());

              previousKeySchema = keySchema;
              previousValueSchemas = valueSchemas;

              keySchema = storeCreation.keySchema.definition.toString();
              valueSchemas = new TreeMap<>();
              valueSchemas.put(1, storeCreation.valueSchema.definition.toString());
            }
          } else if (AdminMessageType.VALUE_SCHEMA_CREATION == adminMessageType) {
            ValueSchemaCreation valueSchemaCreation = (ValueSchemaCreation) adminMessage.payloadUnion;
            if (valueSchemaCreation.storeName.toString().equals(storeName)) {
              System.out.println(
                  "Got value schema creation message for store: " + storeName + " and schema id: "
                      + valueSchemaCreation.schemaId + " with value schema:\n"
                      + valueSchemaCreation.schema.definition.toString());
              if (keySchema == null) {
                throw new VeniceException(
                    "Key schema shouldn't be null when encountering a value schema creation message for store: "
                        + storeName);
              }
              String valueSchemaStr = valueSchemaCreation.schema.definition.toString();
              valueSchemas.put(valueSchemaCreation.schemaId, valueSchemaStr);
            }
          }
        }
      }
    }
    if (keySchema == null) {
      throw new VeniceException(
          "Can't find any admin messages in cluster: " + recoverCluster + " for store: " + storeName);
    }

    // print out store info, key schema and value schema
    StoreJSONSerializer storeJSONSerializer = new StoreJSONSerializer();
    byte[] serializedStoreInfo = storeJSONSerializer.serialize(deletedStore, "");
    System.out.println("Store Info: \n" + new String(serializedStoreInfo));

    if (skipLastStoreCreation) {
      keySchema = previousKeySchema;
      valueSchemas = previousValueSchemas;
    }
    System.out.println("Key schema: \n" + keySchema);
    valueSchemas.forEach((k, v) -> System.out.println("Value schema id: " + k + ", value schema: " + v));

    if (doRepair) {
      /**
       * Do repair.
       * 1. Create store with key/value schema.
       * 2. Register more value schemas.
       * 3. Update store config
       */
      ControllerClient recoveryControllerClient =
          ControllerClientFactory.getControllerClient(recoverCluster, url, sslFactory);
      try {
        System.out.println("Creating a new store for name: " + storeName + " in cluster: " + recoverCluster);
        // Create store
        NewStoreResponse newStoreResponse =
            recoveryControllerClient.createNewStore(storeName, deletedStore.getOwner(), keySchema, valueSchemas.get(1));
        if (newStoreResponse.isError()) {
          throw new VeniceException(
              "Failed to create store: " + storeName + " in cluster: " + recoverCluster + ", and error: "
                  + newStoreResponse.getError());
        }
        // Register new schemas
        valueSchemas.remove(1);
        String finalRecoverCluster = recoverCluster;
        valueSchemas.forEach((k, v) -> {
          System.out.println(
              "Adding a new value schema to store: " + storeName + " in cluster: " + finalRecoverCluster + ":\n " + v);
          SchemaResponse schemaResponse = recoveryControllerClient.addValueSchema(storeName, v);
          if (schemaResponse.isError()) {
            throw new VeniceException(
                "Failed to register value schema to store: " + storeName + " in cluster: " + finalRecoverCluster
                    + ", and error: " + schemaResponse.getError());
          }
        });
        // Update store config
        UpdateStoreQueryParams updateParams = new UpdateStoreQueryParams();
        updateParams.setPartitionCount(deletedStore.getPartitionCount());
        if (deletedStore.getPartitionerConfig() != null) {
          updateParams.setPartitionerClass(deletedStore.getPartitionerConfig().getPartitionerClass())
              .setPartitionerParams(deletedStore.getPartitionerConfig().getPartitionerParams());
        }
        updateParams.setStorageQuotaInByte(deletedStore.getStorageQuotaInByte())
            .setHybridStoreDiskQuotaEnabled(deletedStore.isHybridStoreDiskQuotaEnabled())
            .setReadQuotaInCU(deletedStore.getReadQuotaInCU());
        if (deletedStore.getHybridStoreConfig() != null) {
          updateParams.setHybridRewindSeconds(deletedStore.getHybridStoreConfig().getRewindTimeInSeconds())
              .setHybridOffsetLagThreshold(deletedStore.getHybridStoreConfig().getOffsetLagThresholdToGoOnline())
              .setHybridTimeLagThreshold(
                  deletedStore.getHybridStoreConfig().getProducerTimestampLagThresholdToGoOnlineInSeconds())
              .setHybridDataReplicationPolicy(deletedStore.getHybridStoreConfig().getDataReplicationPolicy())
              .setHybridBufferReplayPolicy((deletedStore.getHybridStoreConfig().getBufferReplayPolicy()));
        }
        if (deletedStore.getEtlStoreConfig() != null) {
          updateParams.setEtledProxyUserAccount(deletedStore.getEtlStoreConfig().getEtledUserProxyAccount());
          updateParams.setRegularVersionETLEnabled(deletedStore.getEtlStoreConfig().isRegularVersionETLEnabled());
          updateParams.setFutureVersionETLEnabled(deletedStore.getEtlStoreConfig().isFutureVersionETLEnabled());
        }
        updateParams.setCompressionStrategy(deletedStore.getCompressionStrategy())
            .setClientDecompressionEnabled(deletedStore.getClientDecompressionEnabled())
            .setChunkingEnabled(deletedStore.isChunkingEnabled())
            .setRmdChunkingEnabled(deletedStore.isRmdChunkingEnabled())
            .setIncrementalPushEnabled(deletedStore.isIncrementalPushEnabled())
            .setBatchGetLimit(deletedStore.getBatchGetLimit())
            .setNumVersionsToPreserve(deletedStore.getNumVersionsToPreserve())
            .setWriteComputationEnabled(deletedStore.isWriteComputationEnabled())
            .setReplicationMetadataVersionID(deletedStore.getRmdVersion())
            .setReadComputationEnabled(deletedStore.isReadComputationEnabled())
            .setBootstrapToOnlineTimeoutInHours(deletedStore.getBootstrapToOnlineTimeoutInHours())
            .setNativeReplicationEnabled(deletedStore.isNativeReplicationEnabled())
            .setPushStreamSourceAddress(deletedStore.getPushStreamSourceAddress())
            .setAutoSchemaPushJobEnabled(deletedStore.isSchemaAutoRegisterFromPushJobEnabled())
            .setBackupStrategy(deletedStore.getBackupStrategy())
            .setBackupVersionRetentionMs(deletedStore.getBackupVersionRetentionMs())
            .setReplicationFactor(deletedStore.getReplicationFactor())
            .setNativeReplicationSourceFabric(deletedStore.getNativeReplicationSourceFabric())
            .setActiveActiveReplicationEnabled(deletedStore.isActiveActiveReplicationEnabled())
            .setStorageNodeReadQuotaEnabled(deletedStore.isStorageNodeReadQuotaEnabled())
            .setMinCompactionLagSeconds(deletedStore.getMinCompactionLagSeconds())
            .setMaxCompactionLagSeconds(deletedStore.getMaxCompactionLagSeconds())
            .setMaxRecordSizeBytes(deletedStore.getMaxRecordSizeBytes())
            .setMaxNearlineRecordSizeBytes(deletedStore.getMaxNearlineRecordSizeBytes())
            .setBlobTransferEnabled(deletedStore.isBlobTransferEnabled())
            .setTargetRegionSwap(deletedStore.getTargetSwapRegion())
            .setTargetRegionSwapWaitTime(deletedStore.getTargetSwapRegionWaitTime())
            .setIsDavinciHeartbeatReported(deletedStore.getIsDavinciHeartbeatReported());
        System.out.println(
            "Updating store: " + storeName + " in cluster: " + recoverCluster + " with params: "
                + updateParams.toString());
        ControllerResponse controllerResponse = recoveryControllerClient.updateStore(storeName, updateParams);
        if (controllerResponse.isError()) {
          throw new VeniceException("Failed to update store: " + storeName + " in cluster: " + recoverCluster);
        }
      } finally {
        recoveryControllerClient.close();
      }
    }
  }

  public static void backupStoreGraveyard(ZkClient zkClient, List<String> graveyardClusterList, String backupFolderPath)
      throws IOException {
    System.out.println("Started backup function");
    // Prepare backup folder
    File backupFolder = new File(backupFolderPath);
    if (!backupFolder.exists()) {
      backupFolder.mkdir();
    } else if (!backupFolder.isDirectory()) {
      throw new VeniceException("Backup folder exists, but not a dir: " + backupFolderPath);
    }
    System.out.println("Backup folder: " + backupFolderPath + " is ready");
    // Create sub dir for each cluster
    for (String clusterName: graveyardClusterList) {
      String folderPathForCluster = backupFolderPath + "/" + clusterName;
      File folderForCluster = new File(folderPathForCluster);
      if (!folderForCluster.exists()) {
        folderForCluster.mkdir();
      } else if (!folderForCluster.isDirectory()) {
        throw new VeniceException("Folder for cluster exists, but not a dir: " + folderPathForCluster);
      }
      System.out.println("Backup folder for cluster: " + clusterName + " is ready");
    }
    System.out.println("Starting extracting stores from graveyard");

    HelixStoreGraveyard helixStoreGraveyard =
        new HelixStoreGraveyard(zkClient, new HelixAdapterSerializer(), graveyardClusterList);
    StoreJSONSerializer storeJSONSerializer = new StoreJSONSerializer();

    for (String clusterName: graveyardClusterList) {
      System.out.println("Started working on cluster: " + clusterName);
      List<String> deletedStoreNames = helixStoreGraveyard.listStoreNamesFromGraveyard(clusterName);
      for (String deletedStoreName: deletedStoreNames) {
        System.out.println("Started working on store: " + deletedStoreName + " in cluster: " + clusterName);
        Store deletedStore = helixStoreGraveyard.getStoreFromGraveyard(clusterName, deletedStoreName, null);
        byte[] serializedDeletedStore = storeJSONSerializer.serialize(deletedStore, "");
        String storePath = backupFolderPath + "/" + clusterName + "/" + deletedStoreName;
        File storeFolder = new File(storePath);
        if (!storeFolder.exists()) {
          storeFolder.mkdir();
        } else if (!storeFolder.isDirectory()) {
          throw new VeniceException("Store folder exists, but not a dir: " + storePath);
        }
        String metadataFilePath = storePath + "/" + "metadata";
        File metadataFile = new File(metadataFilePath);
        if (metadataFile.exists()) {
          metadataFile.delete();
        }
        try (FileOutputStream outputStream = new FileOutputStream(metadataFilePath)) {
          outputStream.write(serializedDeletedStore);
        }
      }
      System.out.println("End working on cluster:" + clusterName);
    }
  }
}
