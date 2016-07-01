package com.linkedin.venice.job;

import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.Partition;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class TestJobStatusDecider {
  protected String topic = "testTopic";
  protected int numberOfPartition = 4;
  protected int replicationFactor = 3;
  protected String nodeId = "localhost_1234";
  protected Map<Integer, Partition> partitions = new HashMap<>();

  protected void createPartitions(int numberOfPartition, int replicationFactor) {
    for (int i = 0; i < numberOfPartition; i++) {
      Partition partition = new Partition(i, topic, createInstances(replicationFactor));
      partitions.put(i, partition);
    }
  }

  protected List<Instance> createInstances(int replicationFactor) {
    List<Instance> instances = new ArrayList<>();
    for (int j = 0; j < replicationFactor; j++) {
      Instance instance = new Instance(nodeId + j, "localhost", 1235);
      instances.add(instance);
    }
    return instances;
  }

  protected void createTasksAndUpdateJob(OfflineJob job, int numberOfPartition, int replicationFactor,
      ExecutionStatus status, int exceptPartition, int exceptReplica) {
    for (int i = 0; i < numberOfPartition; i++) {
      for (int j = 0; j < replicationFactor; j++) {
        Task t = new Task(job.generateTaskId(i, nodeId + j), i, String.valueOf(j), status);
        if (i == exceptPartition && j == exceptReplica) {
          continue;
        }
        job.updateTaskStatus(t);
      }
    }
  }
}
