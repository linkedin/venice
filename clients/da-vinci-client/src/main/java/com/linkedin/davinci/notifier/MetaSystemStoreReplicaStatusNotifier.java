package com.linkedin.davinci.notifier;

import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.system.store.MetaStoreWriter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This notifier is used to report partition replica status change during ingestion.
 */
public class MetaSystemStoreReplicaStatusNotifier implements VeniceNotifier {
  private static final Logger LOGGER = LogManager.getLogger(MetaSystemStoreReplicaStatusNotifier.class);
  private final MetaStoreWriter metaStoreWriter;
  private final String clusterName;
  private final ReadOnlyStoreRepository storeRepository;
  private final Instance instance;

  public MetaSystemStoreReplicaStatusNotifier(
      String clusterName,
      MetaStoreWriter metaStoreWriter,
      ReadOnlyStoreRepository storeRepository,
      Instance instance) {
    this.clusterName = clusterName;
    this.metaStoreWriter = metaStoreWriter;
    this.storeRepository = storeRepository;
    this.instance = instance;
  }

  void report(String kafkaTopic, int partitionId, ExecutionStatus status) {
    String storeName = Version.parseStoreFromKafkaTopicName(kafkaTopic);
    VeniceSystemStoreType systemStoreType = VeniceSystemStoreType.getSystemStoreType(storeName);
    if (systemStoreType != null && systemStoreType.equals(VeniceSystemStoreType.META_STORE)) {
      // No replica status reporting for meta system stores
      return;
    }
    Store store;
    try {
      store = getStoreRepository().getStoreOrThrow(storeName);
    } catch (VeniceNoStoreException e) {
      /**
       * For store deletion, store removal in store repo might happen faster then {@link ExecutionStatus.DROPPED} action.
       * This is to make sure the meta store notifier does not throw exception when this happens. For other status, this
       * is not expected, so we should throw exception.
       */
      if (status.equals(ExecutionStatus.DROPPED)) {
        LOGGER.info(
            "Store {} does not exist in store repository. Skip reporting status: {} for topic: {}, partition: {}",
            storeName,
            status,
            kafkaTopic,
            partitionId);
        return;
      }
      throw e;
    }
    if (!store.isStoreMetaSystemStoreEnabled()) {
      // Meta system store is not enabled yet.
      LOGGER.info("Meta system store for topic: {} is not enabled yet", kafkaTopic);
      return;
    }
    LOGGER.info("Report replica status: {} for topic: {}, partition: {}", status, kafkaTopic, partitionId);
    int version = Version.parseVersionFromKafkaTopicName(kafkaTopic);
    if (status.equals(ExecutionStatus.DROPPED)) {
      try {
        getMetaStoreWriter().deleteStoreReplicaStatus(clusterName, storeName, version, partitionId, instance);
      } catch (Exception e) {
        /**
         * This could potentially happen during store deletion.
         * Since store deletion is a infrequent event, no need to optimize it.
         */
        LOGGER.error(
            "Encountered exception while trying to report `Dropped` status for store: {}, partition: {} in cluster: {}",
            storeName,
            partitionId,
            clusterName,
            e);
      }
    } else {
      getMetaStoreWriter().writeStoreReplicaStatus(clusterName, storeName, version, partitionId, instance, status);
    }
  }

  @Override
  public void started(String kafkaTopic, int partitionId, String message) {
    report(kafkaTopic, partitionId, ExecutionStatus.STARTED);
  }

  @Override
  public void restarted(String kafkaTopic, int partitionId, long offset, String message) {
    report(kafkaTopic, partitionId, ExecutionStatus.STARTED);
  }

  @Override
  public void progress(String kafkaTopic, int partitionId, long offset, String message) {
    report(kafkaTopic, partitionId, ExecutionStatus.PROGRESS);
  }

  @Override
  public void completed(String kafkaTopic, int partitionId, long offset, String message) {
    report(kafkaTopic, partitionId, ExecutionStatus.COMPLETED);
  }

  @Override
  public void error(String kafkaTopic, int partitionId, String message, Exception e) {
    report(kafkaTopic, partitionId, ExecutionStatus.ERROR);
  }

  public void drop(String kafkaTopic, int partitionId) {
    report(kafkaTopic, partitionId, ExecutionStatus.DROPPED);
  }

  MetaStoreWriter getMetaStoreWriter() {
    return metaStoreWriter;
  }

  ReadOnlyStoreRepository getStoreRepository() {
    return storeRepository;
  }
}
