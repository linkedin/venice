package com.linkedin.venice.replication;

import com.linkedin.venice.kafka.TopicManager;
import java.io.Closeable;
import java.util.Map;
import java.util.Optional;


/**
 * Extend this class with an implementation that can start and end replication of data between two kafka topics
 * within the same kafka cluster.
 */
public abstract class TopicReplicator implements Closeable {
  private final TopicManager topicManager;

  public TopicReplicator(TopicManager topicManager){
    this.topicManager = topicManager;
  }

  public void beginReplication(String sourceTopic, String destinationTopic, Optional<Map<Integer, Long>> startingOffsets)
      throws TopicException {
    String errorPrefix = "Cannot create replication datastream from " + sourceTopic + " to " + destinationTopic + " because";
    if (sourceTopic.equals(destinationTopic)){
      throw new DuplicateTopicException(errorPrefix + " they are the same topic.");
    }
    if (!topicManager.containsTopic(sourceTopic)){
      throw new TopicDoesNotExistException(errorPrefix + " topic " + sourceTopic + " does not exist.");
    }
    if (!topicManager.containsTopic(destinationTopic)){
      throw new TopicDoesNotExistException(errorPrefix + " topic " + destinationTopic + " does not exist.");
    }
    int sourcePartitionCount = topicManager.getPartitions(sourceTopic).size();
    int destinationPartitionCount = topicManager.getPartitions(destinationTopic).size();
    if (sourcePartitionCount != destinationPartitionCount){
      throw new PartitionMismatchException(errorPrefix + " topic " + sourceTopic + " has " + sourcePartitionCount + " partitions"
          + " and topic " + destinationTopic + " has " + destinationPartitionCount + " partitions."  );
    }
    beginReplicationInternal(sourceTopic, destinationTopic, sourcePartitionCount, startingOffsets);
  }

  public void terminateReplication(String sourceTopic, String destinationTopic){
    terminateReplicationInternal(sourceTopic, destinationTopic);
  }

  abstract void beginReplicationInternal(String sourceTopic, String destinationTopic, int partitionCount, Optional<Map<Integer, Long>> startingOffsets);
  abstract void terminateReplicationInternal(String sourceTopic, String destinationTopic);

  public static abstract class TopicException extends Exception {
    public TopicException(String message){
      super(message);
    }
  }

  /**
   * The source or destination topic for the replication request does not exit
   */
  public static class TopicDoesNotExistException extends TopicException {
    public TopicDoesNotExistException(String message){
      super(message);
    }
  }

  /**
   * The source and destination topic for the replication are the same topic
   */
  public static class DuplicateTopicException extends TopicException {
    public DuplicateTopicException(String message){
      super(message);
    }
  }

  /**
   * The source and destination topics for replication have different numbers of partitions.
   */
  public static class PartitionMismatchException extends TopicException {
    public PartitionMismatchException(String message){
      super(message);
    }
  }
}
