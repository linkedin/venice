package com.linkedin.venice.replication;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.TopicManager;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.utils.ReflectUtils;
import com.linkedin.venice.utils.SystemTime;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterFactory;

import java.io.Closeable;
import java.util.*;
import java.util.stream.Collectors;


/**
 * Extend this class with an implementation that can start and end replication of data between two kafka topics
 * within the same kafka cluster.
 */
public abstract class TopicReplicator implements Closeable {
  public static final String TOPIC_REPLICATOR_CLASS_NAME = "topic.replicator.class.name";
  public static final String TOPIC_REPLICATOR_SOURCE_KAFKA_CLUSTER = "topic.replicator.source.kafka.cluster";

  private final TopicManager topicManager;
  protected final String destKafkaBootstrapServers;
  private final VeniceWriterFactory veniceWriterFactory;
  private final Time timer;

  /**
   * Intended to allow mocking by tests. Visibility package-private on purpose, but could be changed to
   * protected if child classes need it.
   */
  Time getTimer() {
    return timer;
  }

  /**
   * Intended to allow mocking by tests. Visibility package-private on purpose, but could be changed to
   * protected if child classes need it.
   */
  VeniceWriterFactory getVeniceWriterFactory() {
    return this.veniceWriterFactory;
  }

  private TopicReplicator(TopicManager topicManager, String destKafkaBootstrapServers, VeniceWriterFactory veniceWriterFactory, Time timer) {
    this.topicManager = topicManager;
    this.destKafkaBootstrapServers = destKafkaBootstrapServers;
    this.veniceWriterFactory = veniceWriterFactory;
    this.timer = timer;
  }

  public TopicReplicator(TopicManager topicManager, String destKafkaBootstrapServers) {
    this(topicManager, destKafkaBootstrapServers, VeniceWriterFactory.get(), new SystemTime());
  }

  public void beginReplication(String sourceTopic, String destinationTopic, Optional<Map<Integer, Long>> startingOffsets)
      throws TopicException {
    String errorPrefix = "Cannot create replication datastream from " + sourceTopic + " to " + destinationTopic + " because";
    if (sourceTopic.equals(destinationTopic)){
      throw new DuplicateTopicException(errorPrefix + " they are the same topic.");
    }
    if (!getTopicManager().containsTopic(sourceTopic)){
      throw new TopicDoesNotExistException(errorPrefix + " topic " + sourceTopic + " does not exist.");
    }
    if (!getTopicManager().containsTopic(destinationTopic)){
      throw new TopicDoesNotExistException(errorPrefix + " topic " + destinationTopic + " does not exist.");
    }
    int sourcePartitionCount = getTopicManager().getPartitions(sourceTopic).size();
    int destinationPartitionCount = getTopicManager().getPartitions(destinationTopic).size();
    if (sourcePartitionCount != destinationPartitionCount){
      throw new PartitionMismatchException(errorPrefix + " topic " + sourceTopic + " has " + sourcePartitionCount + " partitions"
          + " and topic " + destinationTopic + " has " + destinationPartitionCount + " partitions."  );
    }
    beginReplicationInternal(sourceTopic, destinationTopic, sourcePartitionCount, startingOffsets);
  }

  public void terminateReplication(String sourceTopic, String destinationTopic){
    terminateReplicationInternal(sourceTopic, destinationTopic);
  }

  public Map<Integer, Long> startBufferReplay(String srcTopicName,
                                              String destTopicName,
                                              Store store) throws TopicException {
    String srcKafkaBootstrapServers = destKafkaBootstrapServers; // TODO: Add this as a function parameter
    // Carrying on assuming that there needs to be only one and only TopicManager

    long bufferReplayStartTime;
    if (store.isHybrid()) {
      bufferReplayStartTime = getTimer().getMilliseconds() - store.getHybridStoreConfig().getRewindTimeInSeconds();
    } else {
      throw new VeniceException("Buffer replay is only supported for Hybrid Stores.");
    }
    Map<Integer, Long> startingOffsetsMap = getTopicManager().getOffsetsByTime(srcTopicName, bufferReplayStartTime);
    List<Long> startingOffsets = startingOffsetsMap.entrySet().stream()
        .sorted((o1, o2) -> o1.getKey().compareTo(o2.getKey()))
        .map(entry -> entry.getValue())
        .collect(Collectors.toList());
    VeniceWriter<byte[], byte[]> veniceWriter = getVeniceWriterFactory().getBasicVeniceWriter(destKafkaBootstrapServers, destTopicName, getTimer());
    veniceWriter.broadcastStartOfBufferReplay(startingOffsets, srcKafkaBootstrapServers, srcTopicName, new HashMap<>());
    beginReplication(srcTopicName, destTopicName, Optional.of(startingOffsetsMap));
    return startingOffsetsMap;
  }

  abstract void beginReplicationInternal(String sourceTopic, String destinationTopic, int partitionCount, Optional<Map<Integer, Long>> startingOffsets);
  abstract void terminateReplicationInternal(String sourceTopic, String destinationTopic);

  public static TopicReplicator getTopicReplicator(TopicManager topicManager, VeniceProperties veniceProperties) {
    String className = veniceProperties.getString(TOPIC_REPLICATOR_CLASS_NAME);

    Class<? extends TopicReplicator> topicReplicatorClass = ReflectUtils.loadClass(className);
    Class<TopicManager> param1Class = ReflectUtils.loadClass(TopicManager.class.getName());
    Class<VeniceProperties> param2Class = ReflectUtils.loadClass(VeniceProperties.class.getName());
    TopicReplicator topicReplicator = ReflectUtils.callConstructor(
        topicReplicatorClass,
        new Class[]{param1Class, param2Class},
        new Object[]{topicManager, veniceProperties});

    return topicReplicator;
  }

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

  /**
   * Package-private visibility intended for mocking in tests.
   */
  TopicManager getTopicManager() {
    return this.topicManager;
  }
}
