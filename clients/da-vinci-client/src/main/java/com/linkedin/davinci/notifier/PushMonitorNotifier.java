package com.linkedin.davinci.notifier;

import static com.linkedin.venice.pushmonitor.ExecutionStatus.COMPLETED;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.DATA_RECOVERY_COMPLETED;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.END_OF_INCREMENTAL_PUSH_RECEIVED;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.END_OF_PUSH_RECEIVED;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.ERROR;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.PROGRESS;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.STARTED;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.START_OF_INCREMENTAL_PUSH_RECEIVED;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.TOPIC_SWITCH_RECEIVED;

import com.linkedin.venice.common.PushStatusStoreUtils;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.pushmonitor.OfflinePushAccessor;
import com.linkedin.venice.pushstatushelper.PushStatusStoreWriter;
import java.util.Optional;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Notifier used to update replica status by offline push monitor accessor.
 */
public class PushMonitorNotifier implements VeniceNotifier {
  private static final Logger LOGGER = LogManager.getLogger(PushMonitorNotifier.class);

  private final OfflinePushAccessor accessor;
  private final PushStatusStoreWriter pushStatuStoreWriter;
  private final ReadOnlyStoreRepository storeRepository;
  private final String instanceId;

  public PushMonitorNotifier(
      OfflinePushAccessor accessor,
      PushStatusStoreWriter pushStatusStoreWriter,
      ReadOnlyStoreRepository storeRepository,
      String instanceId) {
    this.accessor = accessor;
    this.pushStatuStoreWriter = pushStatusStoreWriter;
    this.storeRepository = storeRepository;
    this.instanceId = instanceId;
  }

  @Override
  public void started(String topic, int partitionId, String message) {
    accessor.updateReplicaStatus(topic, partitionId, instanceId, STARTED, "");
  }

  @Override
  public void restarted(String topic, int partitionId, long offset, String message) {
    accessor.updateReplicaStatus(topic, partitionId, instanceId, STARTED, offset, "");
  }

  @Override
  public void completed(String topic, int partitionId, long offset, String message) {
    accessor.updateReplicaStatus(topic, partitionId, instanceId, COMPLETED, offset, "");
  }

  @Override
  public void progress(String topic, int partitionId, long offset, String message) {
    accessor.updateReplicaStatus(topic, partitionId, instanceId, PROGRESS, offset, "");
  }

  @Override
  public void endOfPushReceived(String topic, int partitionId, long offset, String message) {
    accessor.updateReplicaStatus(topic, partitionId, instanceId, END_OF_PUSH_RECEIVED, offset, "");
  }

  @Override
  public void topicSwitchReceived(String topic, int partitionId, long offset, String message) {
    accessor.updateReplicaStatus(topic, partitionId, instanceId, TOPIC_SWITCH_RECEIVED, offset, "");
  }

  @Override
  public void dataRecoveryCompleted(String kafkaTopic, int partitionId, long offset, String message) {
    accessor.updateReplicaStatus(kafkaTopic, partitionId, instanceId, DATA_RECOVERY_COMPLETED, offset, message);
  }

  @Override
  public void startOfIncrementalPushReceived(String topic, int partitionId, long offset, String message) {
    accessor.updateReplicaStatus(topic, partitionId, instanceId, START_OF_INCREMENTAL_PUSH_RECEIVED, offset, message);
    updateIncrementalPushStatus(topic, message, partitionId, START_OF_INCREMENTAL_PUSH_RECEIVED);
  }

  @Override
  public void endOfIncrementalPushReceived(String topic, int partitionId, long offset, String message) {
    accessor.updateReplicaStatus(topic, partitionId, instanceId, END_OF_INCREMENTAL_PUSH_RECEIVED, offset, message);
    updateIncrementalPushStatus(topic, message, partitionId, END_OF_INCREMENTAL_PUSH_RECEIVED);
  }

  private void updateIncrementalPushStatus(
      String kafkaTopic,
      String incPushVersion,
      int partitionId,
      ExecutionStatus status) {
    String storeName = Version.parseStoreFromKafkaTopicName(kafkaTopic);
    try {
      // if push status store doesn't exist do not report inc-push status
      if (!storeRepository.getStoreOrThrow(storeName).isDaVinciPushStatusStoreEnabled()) {
        return;
      }
    } catch (Exception e) {
      LOGGER.error(
          "Failed to report status of incremental push version:{}."
              + " Got an exception while checking whether push status store exist for store:{}",
          incPushVersion,
          storeName,
          e);
    }
    pushStatuStoreWriter.writePushStatus(
        storeName,
        Version.parseVersionFromKafkaTopicName(kafkaTopic),
        partitionId,
        status,
        Optional.of(incPushVersion),
        Optional.of(PushStatusStoreUtils.SERVER_INCREMENTAL_PUSH_PREFIX));
  }

  @Override
  public void close() {
    // Do not need to close here. accessor should be closed by the outer class.
  }

  @Override
  public void error(String topic, int partitionId, String message, Exception ex) {
    accessor.updateReplicaStatus(topic, partitionId, instanceId, ERROR, message);
  }
}
