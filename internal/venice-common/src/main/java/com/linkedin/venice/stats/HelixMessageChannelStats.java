package com.linkedin.venice.stats;

import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Count;
import io.tehuti.metrics.stats.Total;


@Deprecated
public class HelixMessageChannelStats extends AbstractVeniceStats {
  private static final String NAME_SUFFIX = "-helix_message_channel";
  /**
   * Count the number of times when a controller tries to send messages to storage nodes.
   */
  private static final String TO_STORAGE_NODES_INVOKE_COUNT = "to_storage_nodes_invoke_count";
  /**
   * Count the number of times when a on reply call back from a storage node is invoked.
   */
  private static final String ON_REPLY_FROM_STORAGE_NODES_COUNT = "on_reply_from_storage_nodes_count";
  /**
   * Count the number of messages actually sent by a controller.
   */
  private static final String TO_STORAGE_NODES_MESSAGE_COUNT = "to_storage_nodes_message_count";
  /**
   * Count the number of missed storage nodes replies.
   */
  private static final String MISSED_STORAGE_NODES_REPLY_COUNT = "missed_storage_nodes_reply_count";
  private final Sensor toStorageNodesInvokeCountSensor;
  private final Sensor onReplyFromStorageNodesCountSensor;
  private final Sensor toStorageNodesMessageCountSensor;
  private final Sensor missedStorageNodeReplyMessageCountSensor;

  public HelixMessageChannelStats(MetricsRepository metricsRepository, String clusterName) {
    super(metricsRepository, clusterName + NAME_SUFFIX);
    toStorageNodesInvokeCountSensor = registerSensorIfAbsent(TO_STORAGE_NODES_INVOKE_COUNT, new Count());
    onReplyFromStorageNodesCountSensor = registerSensorIfAbsent(ON_REPLY_FROM_STORAGE_NODES_COUNT, new Count());
    toStorageNodesMessageCountSensor = registerSensorIfAbsent(TO_STORAGE_NODES_MESSAGE_COUNT, new Total());
    missedStorageNodeReplyMessageCountSensor = registerSensorIfAbsent(MISSED_STORAGE_NODES_REPLY_COUNT, new Total());
  }

  public void recordToStorageNodesInvokeCount() {
    toStorageNodesInvokeCountSensor.record();
  }

  public void recordOnReplyFromStorageNodesCount() {
    onReplyFromStorageNodesCountSensor.record();
  }

  public void recordToStorageNodesMessageCount(double value) {
    toStorageNodesMessageCountSensor.record(value);
  }

  public void recordMissedStorageNodesReplyCount(double value) {
    missedStorageNodeReplyMessageCountSensor.record(value);
  }
}
