package com.linkedin.venice.unit.kafka;

import static com.linkedin.venice.utils.Time.MS_PER_SECOND;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceRetriableException;
import com.linkedin.venice.kafka.TopicDoesNotExistException;
import com.linkedin.venice.kafka.TopicManager;
import com.linkedin.venice.pubsub.PubSubTopicConfiguration;
import com.linkedin.venice.pubsub.PubSubTopicPartitionInfo;
import com.linkedin.venice.pubsub.api.PubSubAdminAdapter;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.utils.Utils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import org.apache.kafka.common.errors.TimeoutException;


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
  public Future<Void> deleteTopic(PubSubTopic topicName) {
    return CompletableFuture.supplyAsync(() -> {
      topicPubSubTopicConfigurationMap.remove(topicName);
      topicPartitionNumMap.remove(topicName);
      return null;
    });
  }

  @Override
  public Set<PubSubTopic> listAllTopics() {
    return topicPubSubTopicConfigurationMap.keySet();
  }

  @Override
  public void setTopicConfig(PubSubTopic topicName, PubSubTopicConfiguration topicPubSubTopicConfiguration)
      throws TopicDoesNotExistException {
    if (!topicPubSubTopicConfigurationMap.containsKey(topicName)) {
      throw new TopicDoesNotExistException("Topic " + topicName + " does not exist");
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
        retentions.put(entry.getKey(), TopicManager.UNKNOWN_TOPIC_RETENTION);
      }
    }
    return retentions;
  }

  @Override
  public PubSubTopicConfiguration getTopicConfig(PubSubTopic topic) throws TopicDoesNotExistException {
    if (topicPubSubTopicConfigurationMap.containsKey(topic)) {
      return topicPubSubTopicConfigurationMap.get(topic);
    }
    throw new TopicDoesNotExistException("Topic " + topic + " does not exist");
  }

  @Override
  public PubSubTopicConfiguration getTopicConfigWithRetry(PubSubTopic topic) {
    long accumWaitTime = 0;
    long sleepIntervalInMs = 100;
    VeniceException veniceException = null;
    while (accumWaitTime < 1000) {
      try {
        return getTopicConfig(topic);
      } catch (VeniceException e) {
        veniceException = e;
        Utils.sleep(sleepIntervalInMs);
        accumWaitTime += sleepIntervalInMs;
        sleepIntervalInMs = Math.min(5 * MS_PER_SECOND, sleepIntervalInMs * 2);
      }
    }
    throw new VeniceException(
        "After retrying for " + accumWaitTime + "ms, failed to get topic configs for: " + topic,
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
    return Collections.unmodifiableList(Arrays.asList(VeniceRetriableException.class, TimeoutException.class));
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
