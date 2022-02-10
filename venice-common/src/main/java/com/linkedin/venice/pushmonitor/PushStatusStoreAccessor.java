package com.linkedin.venice.pushmonitor;

import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pushstatushelper.PushStatusStoreWriter;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.writer.VeniceWriterFactory;
import java.util.List;
import java.util.Optional;
import org.apache.helix.zookeeper.zkclient.IZkChildListener;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static com.linkedin.venice.ConfigKeys.PUSH_STATUS_STORE_DERIVED_SCHEMA_ID;
import static com.linkedin.venice.common.PushStatusStoreUtils.SERVER_INCREMENTAL_PUSH_PREFIX;


/**
 * An implementation of {@link OfflinePushAccessor} that uses PushStatus system store to
 * save server-side incremental push statuses.
 */
public class PushStatusStoreAccessor implements OfflinePushAccessor {

  private static final Logger logger = LogManager.getLogger(PushStatusStoreAccessor.class);
  private final PushStatusStoreWriter storeWriter;
  private final ReadOnlyStoreRepository storeRepository;

  /**
   * For reporting status using PushStatusStoreAccessor, first we need to have da-Vinci push
   * status store in place. If the push status store does not exist before starting an
   * incremental push job, all of the incremental push-related tests fail. To avoid making
   * changes at too many places, for now, we report status only when PushStatusStore exists.
   * In the future, we will be creating push status stores by default when incremental push
   * is enabled, and we won't check for the existence of PushStatusStore.
   */
  public PushStatusStoreAccessor(VeniceProperties backendProps, ReadOnlyStoreRepository storeRepository, String instanceId) {
    this.storeWriter = new PushStatusStoreWriter(new VeniceWriterFactory(backendProps.toProperties()),
        instanceId, backendProps.getInt(PUSH_STATUS_STORE_DERIVED_SCHEMA_ID, 1));
    this.storeRepository = storeRepository;
  }

  @Override
  public List<OfflinePushStatus> loadOfflinePushStatusesAndPartitionStatuses() {
    throw new UnsupportedOperationException("Method not implemented");
  }

  @Override
  public List<String> loadOfflinePushStatusPaths() {
    throw new UnsupportedOperationException("Method not implemented");
  }

  @Override
  public OfflinePushStatus getOfflinePushStatusAndItsPartitionStatuses(String kafkaTopic) {
    throw new UnsupportedOperationException("Method not implemented");
  }

  @Override
  public void updateOfflinePushStatus(OfflinePushStatus pushStatus) {
    throw new UnsupportedOperationException("Method not implemented");
  }

  @Override
  public void createOfflinePushStatusAndItsPartitionStatuses(OfflinePushStatus pushStatus) {
    throw new UnsupportedOperationException("Method not implemented");
  }

  @Override
  public void deleteOfflinePushStatusAndItsPartitionStatuses(String kafkaTopic) {
    throw new UnsupportedOperationException("Method not implemented");
  }

  @Override
  public void updateReplicaStatus(String kafkaTopic, int partitionId, String instanceId, ExecutionStatus status,
      long progress, String message) {
    updateReplicaStatus(kafkaTopic, partitionId, instanceId, status, message);
  }

  @Override
  public void updateReplicaStatus(String kafkaTopic, int partitionId, String instanceId, ExecutionStatus status, String message) {
    String storeName = Version.parseStoreFromKafkaTopicName(kafkaTopic);
    try {
      // report status only if Da-Vinci Push Status Store exist
      if (storeRepository.getStoreOrThrow(storeName).isDaVinciPushStatusStoreEnabled()) {
        storeWriter.writePushStatus(storeName, Version.parseVersionFromKafkaTopicName(kafkaTopic), partitionId,
            status, Optional.of(message), Optional.of(SERVER_INCREMENTAL_PUSH_PREFIX));
      }
    } catch (Exception e) {
      logger.error("Failed to report server incremental push status. KafkaTopic:{} PartitionId:{} Status:{}",
          kafkaTopic, partitionId, status.name(), e);
    }
  }

  @Override
  public void updateReplicaHighWatermarkStatus(String kafkaTopic, int partitionId, String instanceId,
      long highWatermark, String message) {
    String storeName = Version.parseStoreFromKafkaTopicName(kafkaTopic);
    try {
      // report status only if Da-Vinci Push Status Store exist
      if (!storeRepository.getStoreOrThrow(storeName).isDaVinciPushStatusStoreEnabled()) {
        return;
      }
      storeWriter.writeHighWatermark(storeName, Version.parseVersionFromKafkaTopicName(kafkaTopic), partitionId,
          highWatermark, Optional.of(message), Optional.of(SERVER_INCREMENTAL_PUSH_PREFIX));
    } catch (Exception e) {
      logger.error("Failed to report EOIP high watermark status. KafkaTopic:{} PartitionId:{} ",
          kafkaTopic, partitionId, e);
    }
  }

  @Override
  public void subscribePartitionStatusChange(OfflinePushStatus pushStatus, PartitionStatusListener listener) {
    throw new UnsupportedOperationException("Method not implemented");
  }

  @Override
  public void unsubscribePartitionsStatusChange(OfflinePushStatus pushStatus, PartitionStatusListener listener) {
    throw new UnsupportedOperationException("Method not implemented");
  }

  @Override
  public void unsubscribePartitionsStatusChange(String topicName, int partitionCount,
      PartitionStatusListener listener) {
    throw new UnsupportedOperationException("Method not implemented");
  }

  @Override
  public void subscribePushStatusCreationChange(IZkChildListener childListener) {
    throw new UnsupportedOperationException("Method not implemented");
  }

  @Override
  public void unsubscribePushStatusCreationChange(IZkChildListener childListener) {
    throw new UnsupportedOperationException("Method not implemented");
  }
}
