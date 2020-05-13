package com.linkedin.venice.kafka.consumer;

import com.linkedin.venice.exceptions.UnsubscribedTopicPartitionException;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.Logger;


/**
 * This class is a synchronized version of {@link KafkaConsumerWrapper}.
 * Especially, this class adds the special support for function: {@link #close(Set)} since this consumer could
 * subscript to multiple topics and the customer could only remove the subscriptions belonging to the specified
 * topics. Also the support for function: {@link #hasSubscription(Set)} since it could be shared by multiple users.
 *
 * In addition to the existing API of {@link KafkaConsumerWrapper}, this class also adds specific functions: {@link #attach},
 * {@link #detach}, which will be used by {@link KafkaConsumerService}.
 */
public class SharedKafkaConsumer implements KafkaConsumerWrapper {
  private static final Logger LOGGER = Logger.getLogger(SharedKafkaConsumer.class);

  private final KafkaConsumerWrapper delegate;
  /**
   * This cached assignment is for performance optimization purpose since {@link #hasSubscription} could be invoked frequently.
   */
  private Set<TopicPartition> currentAssignment = Collections.emptySet();

  /**
   * This field contains the mapping between subscribed topics and the corresponding {@link StoreIngestionTask} to handle
   * all the messages from those topics.
   * The reason to maintain a mapping here since different {@link SharedKafkaConsumer} could subscribe the same topic, and
   * one use case is Hybrid store, and multiple store versions will consume the same real-time topic.
   */
  private final Map<String, StoreIngestionTask> topicToIngestionTaskMap = new VeniceConcurrentHashMap<>();

  public SharedKafkaConsumer(final KafkaConsumerWrapper delegate) {
    this.delegate = delegate;
  }

  /**
   * If {@param isClosed} is true, this function will update the {@link #currentAssignment} to be empty.
   * @param isClosed
   */
  private void updateCurrentAssignment(boolean isClosed) {
    if (isClosed) {
      this.currentAssignment = Collections.emptySet();
    } else {
      this.currentAssignment = this.delegate.getAssignment();
    }
  }

  @Override
  public synchronized void subscribe(String topic, int partition, long lastReadOffset) {
    this.delegate.subscribe(topic, partition, lastReadOffset);
    updateCurrentAssignment(false);
  }

  @Override
  public synchronized void unSubscribe(String topic, int partition) {
    this.delegate.unSubscribe(topic, partition);
    updateCurrentAssignment(false);
  }

  @Override
  public synchronized void resetOffset(String topic, int partition) throws UnsubscribedTopicPartitionException {
    this.delegate.resetOffset(topic, partition);
  }

  @Override
  public synchronized void close() {
    this.delegate.close();
    updateCurrentAssignment(true);
  }

  @Override
  public synchronized void close(Set<String> topics) {
    // Get the current subscription for this topic and unsubscribe them
    Set<TopicPartition> currentAssignment = getAssignment();
    List<TopicPartition> newAssignment = new LinkedList<>();
    for (TopicPartition topicPartition : currentAssignment) {
      if (!topics.contains(topicPartition.topic())) {
        newAssignment.add(topicPartition);
      }
    }
    if (newAssignment.size() == currentAssignment.size()) {
      // nothing changed.
      return;
    }
    this.delegate.assign(newAssignment);
    updateCurrentAssignment(false);
  }

  @Override
  public synchronized ConsumerRecords<KafkaKey, KafkaMessageEnvelope> poll(long timeout) {
    return this.delegate.poll(timeout);
  }

  @Override
  public synchronized boolean hasSubscription() {
    return !this.currentAssignment.isEmpty();
  }

  /**
   * This function will return true as long as any topic in the passed {@param topics} has been subscribed.
   *
   * @param topics
   * @return
   */
  @Override
  public synchronized boolean hasSubscription(Set<String> topics) {
    for (TopicPartition topicPartition : currentAssignment) {
      if (topics.contains(topicPartition.topic())) {
        return true;
      }
    }
    return false;
  }


  @Override
  public synchronized boolean hasSubscription(String topic, int partition) {
    return this.currentAssignment.contains(new TopicPartition(topic, partition));
  }

  @Override
  public synchronized Map<String, List<PartitionInfo>> listTopics() {
    return this.delegate.listTopics();
  }

  @Override
  public synchronized Map<TopicPartition, Long> beginningOffsets(List<TopicPartition> topicPartitions) {
    return this.delegate.beginningOffsets(topicPartitions);
  }

  @Override
  public synchronized Map<TopicPartition, Long> endOffsets(List<TopicPartition> topicPartitions) {
    return this.delegate.endOffsets(topicPartitions);
  }

  @Override
  public synchronized void assign(List<TopicPartition> topicPartitions) {
    this.delegate.assign(topicPartitions);
    updateCurrentAssignment(false);
  }

  @Override
  public synchronized void seek(TopicPartition topicPartition, long nextOffset) {
    this.delegate.seek(topicPartition, nextOffset);
  }

  @Override
  public synchronized void pause(String topic, int partition) {
    this.delegate.pause(topic, partition);
  }

  @Override
  public synchronized void resume(String topic, int partition) {
    this.delegate.resume(topic, partition);
  }

  @Override
  public synchronized Set<TopicPartition> paused() {
    return this.delegate.paused();
  }

  @Override
  public synchronized Set<TopicPartition> getAssignment() {
    return Collections.unmodifiableSet(currentAssignment);
  }

  /**
   * Attach the messages belonging to {@param topic} to the passed {@param ingestionTask}
   * @param topic
   * @param ingestionTask
   */
  public void attach(String topic, StoreIngestionTask ingestionTask) {
    topicToIngestionTaskMap.put(topic, ingestionTask);
    LOGGER.info("Attached the message processing of topic: " + topic + " to the ingestion task belonging to version topic: "
        + ingestionTask.getVersionTopic());
  }

  /**
   * Detach the messages processing belonging to the topics of the passed {@param ingestionTask}
   * @param ingestionTask
   */
  public void detach(StoreIngestionTask ingestionTask) {
    Set<String> subscribedTopics = ingestionTask.getEverSubscribedTopics();
    subscribedTopics.forEach(topic -> topicToIngestionTaskMap.remove(topic));
    LOGGER.info("Detached ingestion task, which has subscribed topics: " + subscribedTopics);
  }

  /**
   * Get the corresponding {@link StoreIngestionTask} for the subscribed topic.
   * @param topic
   * @return
   */
  public StoreIngestionTask getIngestionTaskForTopic(String topic) {
    return topicToIngestionTaskMap.get(topic);
  }
}
