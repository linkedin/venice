package com.linkedin.davinci.notifier;

import static com.linkedin.venice.pushmonitor.ExecutionStatus.*;

import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pushmonitor.OfflinePushAccessor;
import com.linkedin.venice.pushstatushelper.PushStatusStoreWriter;


public class LeaderForceFailNotifier extends PushMonitorNotifier {
  public static boolean setLeaderToError = false;

  public LeaderForceFailNotifier(
      OfflinePushAccessor accessor,
      PushStatusStoreWriter pushStatusStoreWriter,
      ReadOnlyStoreRepository storeRepository,
      String instanceId) {
    super(accessor, pushStatusStoreWriter, storeRepository, instanceId);
  }

  @Override
  public void completed(String topic, int partitionId, long offset, String message) {
    if (VeniceSystemStoreUtils.isSystemStore(Version.parseStoreFromKafkaTopicName(topic))) {
      accessor.updateReplicaStatus(topic, partitionId, instanceId, COMPLETED, offset, "");
      return;
    }
    if (!setLeaderToError && !VeniceSystemStoreUtils.isSystemStore(Version.parseStoreFromKafkaTopicName(topic))) {
      accessor.updateReplicaStatus(topic, partitionId, instanceId, ERROR, offset, "");
      setLeaderToError = true;
    } else {
      accessor.updateReplicaStatus(topic, partitionId, instanceId, COMPLETED, offset, "");
    }
  }

}
