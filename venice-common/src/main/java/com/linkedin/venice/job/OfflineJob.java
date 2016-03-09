package com.linkedin.venice.job;

import com.linkedin.venice.meta.OfflinePushStrategy;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * Created by yayan on 3/7/16.
 */
public class OfflineJob extends Job {
  private Map<Integer, List<Task>> partitionToTasksMap;
  private OfflinePushStrategy strategy;

  public OfflineJob(String kafkaTopic, int numberOfPartition, int replicaFactor) {
    this(kafkaTopic, numberOfPartition, replicaFactor, OfflinePushStrategy.WAIT_ALL_REPLICAS);
  }

  public OfflineJob(String kafkaTopic, int numberOfPartition, int replicaFactor, OfflinePushStrategy strategy) {
    super(kafkaTopic, numberOfPartition, replicaFactor);
    this.strategy = strategy;
    this.partitionToTasksMap = new HashMap<>();
  }



}
