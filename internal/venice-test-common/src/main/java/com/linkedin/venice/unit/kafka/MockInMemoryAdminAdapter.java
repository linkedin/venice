package com.linkedin.venice.unit.kafka;

import static com.linkedin.venice.utils.Time.MS_PER_SECOND;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceRetriableException;
import com.linkedin.venice.pubsub.PubSubConstants;
import com.linkedin.venice.pubsub.PubSubTopicConfiguration;
import com.linkedin.venice.pubsub.PubSubTopicPartitionInfo;
import com.linkedin.venice.pubsub.api.PubSubAdminAdapter;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.pubsub.api.exceptions.PubSubClientRetriableException;
import com.linkedin.venice.pubsub.api.exceptions.PubSubOpTimeoutException;
import com.linkedin.venice.pubsub.api.exceptions.PubSubTopicDoesNotExistException;
import com.linkedin.venice.utils.Utils;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;


public class MockInMemoryAdminAdapter implements PubSubAdminAdapter {
  private final Map<PubSubTopic, PubSubTopicConfiguration> topicPubSubTopicConfigurationMap = new HashMap<>();
  private final Map<PubSubTopic, List<PubSubTopicPartitionInfo>> topicPartitionNumMap = new HashMap<>();

  private final InMemoryKafkaBroker inMemoryKafkaBroker;

  public MockInMemoryAdminAdapter(InMemoryKafkaBroker inMemoryKafkaBroker) {
    this.inMemoryKafkaBroker = inMemoryKafkaBroker;
  }

  @Override
  public void createTopic(
      PubSubTopic topic,
      int numPartitions,
      int replication,
      PubSubTopicConfiguration topicPubSubTopicConfiguration) {
    if (!topicPubSubTopicConfigurationMap.containsKey(topic)) {
      inMemoryKafkaBroker.createTopic(topic.getName(), numPartitions);
    }
    // Emulates kafka default setting.
    if (!topicPubSubTopicConfiguration.minInSyncReplicas().isPresent()) {
      topicPubSubTopicConfiguration.setMinInSyncReplicas(Optional.of(1));
    }
    topicPubSubTopicConfigurationMap.put(topic, topicPubSubTopicConfiguration);
    topicPartitionNumMap.put(topic, new ArrayList<>());
    for (int i = 0; i < numPartitions; i++) {
      topicPartitionNumMap.get(topic).add(new PubSubTopicPartitionInfo(topic, i, true));
    }
  }

  @Override
  public void deleteTopic(PubSubTopic topicName, Duration timeout) {
    topicPubSubTopicConfigurationMap.remove(topicName);
    topicPartitionNumMap.remove(topicName);
  }

  @Override
  public Set<PubSubTopic> listAllTopics() {
    return topicPubSubTopicConfigurationMap.keySet();
  }

  @Override
  public void setTopicConfig(PubSubTopic topicName, PubSubTopicConfiguration topicPubSubTopicConfiguration)
      throws PubSubTopicDoesNotExistException {
    if (!topicPubSubTopicConfigurationMap.containsKey(topicName)) {
      throw new PubSubTopicDoesNotExistException("Topic " + topicName + " does not exist");
    }
    topicPubSubTopicConfigurationMap.put(topicName, topicPubSubTopicConfiguration);
  }

  @Override
  public Map<PubSubTopic, Long> getAllTopicRetentions() {
    Map<PubSubTopic, Long> retentions = new HashMap<>();
    for (Map.Entry<PubSubTopic, PubSubTopicConfiguration> entry: topicPubSubTopicConfigurationMap.entrySet()) {
      PubSubTopicConfiguration topicConfig = entry.getValue();
      Optional<Long> retentionMs = topicConfig.retentionInMs();
      if (retentionMs.isPresent()) {
        retentions.put(entry.getKey(), retentionMs.get());
      } else {
        retentions.put(entry.getKey(), PubSubConstants.PUBSUB_TOPIC_UNKNOWN_RETENTION);
      }
    }
    return retentions;
  }

  @Override
  public PubSubTopicConfiguration getTopicConfig(PubSubTopic topic) throws PubSubTopicDoesNotExistException {
    if (topicPubSubTopicConfigurationMap.containsKey(topic)) {
      return topicPubSubTopicConfigurationMap.get(topic);
    }
    throw new PubSubTopicDoesNotExistException("Topic " + topic + " does not exist");
  }

  @Override
  public PubSubTopicConfiguration getTopicConfigWithRetry(PubSubTopic pubSubTopic) {
    long accumWaitTime = 0;
    long sleepIntervalInMs = 100;
    VeniceException veniceException = null;
    while (accumWaitTime < 1000) {
      try {
        return getTopicConfig(pubSubTopic);
      } catch (VeniceException e) {
        veniceException = e;
        Utils.sleep(sleepIntervalInMs);
        accumWaitTime += sleepIntervalInMs;
        sleepIntervalInMs = Math.min(5 * MS_PER_SECOND, sleepIntervalInMs * 2);
      }
    }
    throw new VeniceException(
        "After retrying for " + accumWaitTime + "ms, failed to get topic configs for: " + pubSubTopic,
        veniceException);
  }

  @Override
  public boolean containsTopic(PubSubTopic topic) {
    return topicPubSubTopicConfigurationMap.containsKey(topic);
  }

  @Override
  public boolean containsTopicWithPartitionCheck(PubSubTopicPartition pubSubTopicPartition) {
    if (topicPartitionNumMap.containsKey(pubSubTopicPartition.getPubSubTopic())) {
      return topicPartitionNumMap.get(pubSubTopicPartition.getPubSubTopic()).size() > pubSubTopicPartition
          .getPartitionNumber();
    }
    return false;
  }

  @Override
  public List<Class<? extends Throwable>> getRetriableExceptions() {
    return Collections.unmodifiableList(
        Arrays.asList(
            VeniceRetriableException.class,
            PubSubOpTimeoutException.class,
            PubSubClientRetriableException.class));
  }

  @Override
  public Map<PubSubTopic, PubSubTopicConfiguration> getSomeTopicConfigs(Set<PubSubTopic> topicNames) {
    Map<PubSubTopic, PubSubTopicConfiguration> topicConfigs = new HashMap<>();
    for (PubSubTopic topic: topicNames) {
      if (topicPubSubTopicConfigurationMap.containsKey(topic)) {
        topicConfigs.put(topic, topicPubSubTopicConfigurationMap.get(topic));
      }
    }
    return topicConfigs;
  }

  public List<PubSubTopicPartitionInfo> partitionsFor(PubSubTopic topic) {
    return topicPartitionNumMap.get(topic);
  }

  @Override
  public String getClassName() {
    return this.getClass().getName();
  }

  @Override
  public void close() throws IOException {
    topicPubSubTopicConfigurationMap.clear();
    topicPartitionNumMap.clear();
  }
}
