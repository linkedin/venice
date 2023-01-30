package com.linkedin.davinci.kafka.consumer;

import com.linkedin.venice.exceptions.UnsubscribedTopicPartitionException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.pubsub.PubSubTopicImpl;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.pubsub.consumer.PubSubConsumer;
import java.util.HashSet;
import java.util.Set;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;


public class SharedPubSubConsumerImpl implements PubSubConsumer {
  SharedKafkaConsumer delegateSharedKafkaConsumer;

  public SharedPubSubConsumerImpl(SharedKafkaConsumer sharedKafkaConsumer) {
    this.delegateSharedKafkaConsumer = sharedKafkaConsumer;
  }

  @Override
  public void subscribe(PubSubTopicPartition pubSubTopicPartition, long lastReadOffset) {
    throw new VeniceException(
        this.getClass().getSimpleName() + " does not support subscribe without specifying a version-topic.");
  }

  public void subscribe(String versionTopic, PubSubTopicPartition topicPartitionToSubscribe, long lastReadOffset) {
    delegateSharedKafkaConsumer.subscribe(
        versionTopic,
        new TopicPartition(
            topicPartitionToSubscribe.getPubSubTopic().getName(),
            topicPartitionToSubscribe.getPartitionNumber()),
        lastReadOffset);
  }

  @Override
  public void unSubscribe(PubSubTopicPartition pubSubTopicPartition) {
    delegateSharedKafkaConsumer
        .unSubscribe(pubSubTopicPartition.getPubSubTopic().getName(), pubSubTopicPartition.getPartitionNumber());
  }

  @Override
  public void batchUnsubscribe(Set<PubSubTopicPartition> pubSubTopicPartitionSet) {
    Set<TopicPartition> topicPartitionSet = new HashSet<>();
    pubSubTopicPartitionSet.forEach(
        pubSubTopicPartition -> topicPartitionSet.add(
            new TopicPartition(
                pubSubTopicPartition.getPubSubTopic().getName(),
                pubSubTopicPartition.getPartitionNumber())));
    delegateSharedKafkaConsumer.batchUnsubscribe(topicPartitionSet);
  }

  @Override
  public void resetOffset(PubSubTopicPartition pubSubTopicPartition) throws UnsubscribedTopicPartitionException {
    delegateSharedKafkaConsumer
        .unSubscribe(pubSubTopicPartition.getPubSubTopic().getName(), pubSubTopicPartition.getPartitionNumber());
  }

  @Override
  public void close() {
    delegateSharedKafkaConsumer.close();
  }

  @Override
  public ConsumerRecords<byte[], byte[]> poll(long timeoutMs) {
    return delegateSharedKafkaConsumer.poll(timeoutMs);
  }

  @Override
  public boolean hasAnySubscription() {
    return delegateSharedKafkaConsumer.hasAnySubscription();
  }

  @Override
  public boolean hasSubscription(PubSubTopicPartition pubSubTopicPartition) {
    return delegateSharedKafkaConsumer
        .hasSubscription(pubSubTopicPartition.getPubSubTopic().getName(), pubSubTopicPartition.getPartitionNumber());
  }

  @Override
  public void pause(PubSubTopicPartition pubSubTopicPartition) {
    delegateSharedKafkaConsumer
        .pause(pubSubTopicPartition.getPubSubTopic().getName(), pubSubTopicPartition.getPartitionNumber());
  }

  @Override
  public void resume(PubSubTopicPartition pubSubTopicPartition) {
    delegateSharedKafkaConsumer
        .resume(pubSubTopicPartition.getPubSubTopic().getName(), pubSubTopicPartition.getPartitionNumber());
  }

  @Override
  public Set<PubSubTopicPartition> getAssignment() {
    Set<TopicPartition> topicPartitionSet = delegateSharedKafkaConsumer.getAssignment();
    Set<PubSubTopicPartition> pubSubTopicPartitionSet = new HashSet<>();
    topicPartitionSet.forEach(
        topicPartition -> pubSubTopicPartitionSet.add(
            new PubSubTopicPartitionImpl(new PubSubTopicImpl(topicPartition.topic()), topicPartition.partition())));
    return pubSubTopicPartitionSet;
  }

  public int getAssignmentSize() {
    return delegateSharedKafkaConsumer.getAssignmentSize();
  }

  public void setCurrentAssignment(Set<PubSubTopicPartition> consumer1AssignedPartitions) {
    Set<TopicPartition> topicPartitionSet = new HashSet<>();
    consumer1AssignedPartitions.forEach(
        pubSubTopicPartition -> topicPartitionSet.add(
            new TopicPartition(
                pubSubTopicPartition.getPubSubTopic().getName(),
                pubSubTopicPartition.getPartitionNumber())));
    delegateSharedKafkaConsumer.setCurrentAssignment(topicPartitionSet);
  }
}
