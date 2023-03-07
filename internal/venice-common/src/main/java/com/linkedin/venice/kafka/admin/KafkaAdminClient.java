package com.linkedin.venice.kafka.admin;

import static com.linkedin.venice.ConfigKeys.KAFKA_ADMIN_GET_TOPIC_CONFIG_MAX_RETRY_TIME_SEC;
import static com.linkedin.venice.utils.Time.MS_PER_SECOND;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceRetriableException;
import com.linkedin.venice.kafka.TopicDoesNotExistException;
import com.linkedin.venice.kafka.TopicManager;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.pubsub.ImmutablePubSubMessage;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicPartitionInfo;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.serialization.KafkaKeySerializer;
import com.linkedin.venice.serialization.avro.OptimizedKafkaValueSerializer;
import com.linkedin.venice.utils.Utils;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.InvalidReplicationFactorException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class KafkaAdminClient implements KafkaAdminWrapper {
  private static final Logger LOGGER = LogManager.getLogger(KafkaAdminClient.class);
  private AdminClient kafkaAdminClient;
  private KafkaConsumer<KafkaKey, KafkaMessageEnvelope> kafkaConsumer;
  private Long maxRetryInMs;

  private PubSubTopicRepository pubSubTopicRepository;

  public KafkaAdminClient() {
  }

  @Override
  public void initialize(Properties properties, PubSubTopicRepository pubSubTopicRepository) {
    if (properties == null) {
      throw new IllegalArgumentException("properties cannot be null!");
    }
    this.kafkaAdminClient = AdminClient.create(properties);
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaKeySerializer.class);
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, OptimizedKafkaValueSerializer.class);
    properties.put(ConsumerConfig.RECEIVE_BUFFER_CONFIG, 1024 * 1024);
    this.kafkaConsumer = new KafkaConsumer<>(properties);
    this.maxRetryInMs = (Long) properties.get(KAFKA_ADMIN_GET_TOPIC_CONFIG_MAX_RETRY_TIME_SEC) * MS_PER_SECOND;
    this.pubSubTopicRepository = pubSubTopicRepository;
  }

  @Override
  public void createTopic(PubSubTopic topicName, int numPartitions, int replication, Properties topicProperties) {
    if (replication > Short.MAX_VALUE) {
      throw new IllegalArgumentException("Replication factor cannot be > " + Short.MAX_VALUE);
    }
    Map<String, String> topicPropertiesMap = new HashMap<>();
    topicProperties.stringPropertyNames().forEach(key -> topicPropertiesMap.put(key, topicProperties.getProperty(key)));
    Collection<NewTopic> newTopics = Collections
        .singleton(new NewTopic(topicName.getName(), numPartitions, (short) replication).configs(topicPropertiesMap));
    try {
      getKafkaAdminClient().createTopics(newTopics).all().get();
    } catch (ExecutionException e) {
      if (e.getCause() instanceof InvalidReplicationFactorException) {
        throw (InvalidReplicationFactorException) e.getCause();
      } else if (e.getCause() instanceof TopicExistsException) {
        throw (TopicExistsException) e.getCause();
      } else {
        throw new VeniceException("Failed to create topic: " + topicName + " due to ExecutionException", e);
      }
    } catch (Exception e) {
      throw new VeniceException("Failed to create topic: " + topicName + "due to Exception", e);
    }
  }

  @Override
  public Set<PubSubTopic> listAllTopics() {
    ListTopicsResult listTopicsResult = getKafkaAdminClient().listTopics();
    try {
      return listTopicsResult.names()
          .get()
          .stream()
          .map(t -> pubSubTopicRepository.getTopic(t))
          .collect(Collectors.toSet());
    } catch (Exception e) {
      throw new VeniceException("Failed to list all topics due to exception: ", e);
    }
  }

  // TODO: If we decide that topic deletion is always going to be blocking then we might want to get the future here and
  // catch/extract any expected exceptions such as UnknownTopicOrPartitionException.
  @Override
  public KafkaFuture<Void> deleteTopic(PubSubTopic topicName) {
    return getKafkaAdminClient().deleteTopics(Collections.singleton(topicName.getName()))
        .values()
        .get(topicName.getName());
  }

  @Override
  public void setTopicConfig(PubSubTopic topicName, Properties topicProperties) throws TopicDoesNotExistException {
    Collection<ConfigEntry> entries = new ArrayList<>(topicProperties.stringPropertyNames().size());
    topicProperties.stringPropertyNames()
        .forEach(key -> entries.add(new ConfigEntry(key, topicProperties.getProperty(key))));
    Map<ConfigResource, Config> configs = Collections
        .singletonMap(new ConfigResource(ConfigResource.Type.TOPIC, topicName.getName()), new Config(entries));
    try {
      getKafkaAdminClient().alterConfigs(configs).all().get();
    } catch (ExecutionException | InterruptedException e) {
      if (!containsTopicWithExpectationAndRetry(topicName, 3, true)) {
        // We assume the exception was caused by a non-existent topic.
        throw new TopicDoesNotExistException("Topic " + topicName + " does not exist");
      }
      // Topic exists. So not sure what caused the exception.
      throw new VeniceException(e);
    }
  }

  @Override
  public Map<PubSubTopic, Long> getAllTopicRetentions() {
    return getSomethingForAllTopics(
        config -> Optional.ofNullable(config.get(TopicConfig.RETENTION_MS_CONFIG))
            // Option A: perform a string-to-long conversion if it's present...
            .map(configEntry -> Long.parseLong(configEntry.value()))
            // Option B: ... or default to a sentinel value if it's missing
            .orElse(TopicManager.UNKNOWN_TOPIC_RETENTION),
        "retention");
  }

  @Override
  public Properties getTopicConfig(PubSubTopic topicName) throws TopicDoesNotExistException {
    ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topicName.getName());
    Collection<ConfigResource> configResources = Collections.singleton(resource);
    DescribeConfigsResult result = getKafkaAdminClient().describeConfigs(configResources);
    try {
      Config config = result.all().get().get(resource);
      Properties properties = new Properties();
      config.entries().forEach(configEntry -> properties.setProperty(configEntry.name(), configEntry.value()));
      return properties;
    } catch (Exception e) {
      if (e.getCause() instanceof UnknownTopicOrPartitionException) {
        throw new TopicDoesNotExistException("Topic: " + topicName + " doesn't exist");
      }
      throw new VeniceException("Failed to get topic configs for: " + topicName, e);
    }
  }

  @Override
  public Properties getTopicConfigWithRetry(PubSubTopic topic) {
    long accumWaitTime = 0;
    long sleepIntervalInMs = 100;
    VeniceException veniceException = null;
    while (accumWaitTime < this.maxRetryInMs) {
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
    try {
      Collection<String> topicNames = Collections.singleton(topic.getName());
      TopicDescription topicDescription =
          getKafkaAdminClient().describeTopics(topicNames).values().get(topic.getName()).get();

      if (topicDescription == null) {
        LOGGER.warn(
            "Unexpected: kafkaAdminClient.describeTopics returned null "
                + "(rather than throwing an InvalidTopicException). Will carry on assuming the topic doesn't exist.");
        return false;
      }

      return true;
    } catch (ExecutionException e) {
      if (e.getCause() instanceof UnknownTopicOrPartitionException || e.getCause() instanceof InvalidTopicException) {
        // Topic doesn't exist...
        return false;
      } else {
        throw new VeniceException("Failed to check if '" + topic + " exists!", e);
      }
    } catch (Exception e) {
      throw new VeniceException("Failed to check if '" + topic + " exists!", e);
    }
  }

  @Override
  public boolean containsTopicWithPartitionCheck(PubSubTopicPartition pubSubTopicPartition) {
    PubSubTopic pubSubTopic = pubSubTopicPartition.getPubSubTopic();
    int partitionID = pubSubTopicPartition.getPartitionNumber();
    try {

      Collection<String> topicNames = Collections.singleton(pubSubTopic.getName());
      TopicDescription topicDescription =
          getKafkaAdminClient().describeTopics(topicNames).values().get(pubSubTopic.getName()).get();

      if (topicDescription == null) {
        LOGGER.warn(
            "Unexpected: kafkaAdminClient.describeTopics returned null "
                + "(rather than throwing an InvalidTopicException). Will carry on assuming the topic doesn't exist.");
        return false;
      }

      if (topicDescription.partitions().size() <= partitionID) {
        LOGGER.warn(
            "{} is trying to check partitionID {}, but total partitions count is {}. "
                + "Will carry on assuming the topic doesn't exist.",
            pubSubTopic,
            partitionID,
            topicDescription.partitions().size());
        return false;
      }
      return true;
    } catch (Exception e) {
      if (e.getCause() instanceof UnknownTopicOrPartitionException || e.getCause() instanceof InvalidTopicException) {
        // Topic doesn't exist...
        return false;
      } else {
        throw new VeniceException("Failed to check if '" + pubSubTopic + " exists!", e);
      }
    }
  }

  @Override
  public List<Class<? extends Throwable>> getRetriableExceptions() {
    return Collections.unmodifiableList(Arrays.asList(VeniceRetriableException.class, TimeoutException.class));
  }

  @Override
  public Map<PubSubTopic, Properties> getSomeTopicConfigs(Set<PubSubTopic> topicNames) {
    return getSomethingForSomeTopics(topicNames, config -> marshallProperties(config), "configs");
  }

  // TODO: This method should be removed from the interface once we migrate to use Java kafka admin since we no longer
  // need to know if a topic deletion is underway or not. Duplicate calls in the Java kafka admin will not result
  // errors.
  @Override
  public boolean isTopicDeletionUnderway() {
    // Always return false to bypass the checks since concurrent topic delete request for Java kafka admin client is
    // harmless.
    return false;
  }

  @Override
  public String getClassName() {
    return KafkaAdminClient.class.getName();
  }

  @Override
  public Long offsetForTime(PubSubTopicPartition pubSubTopicPartition, long timestamp, Duration timeout) {
    TopicPartition topicPartition =
        new TopicPartition(pubSubTopicPartition.getPubSubTopic().getName(), pubSubTopicPartition.getPartitionNumber());
    Map<TopicPartition, OffsetAndTimestamp> topicPartitionOffsetMap =
        this.kafkaConsumer.offsetsForTimes(Collections.singletonMap(topicPartition, timestamp), timeout);
    if (topicPartitionOffsetMap.isEmpty()) {
      return -1L;
    }
    OffsetAndTimestamp offsetAndTimestamp = topicPartitionOffsetMap.get(topicPartition);
    if (offsetAndTimestamp == null) {
      return null;
    }
    return offsetAndTimestamp.offset();
  }

  @Override
  public Long offsetForTime(PubSubTopicPartition pubSubTopicPartition, long timestamp) {
    TopicPartition topicPartition =
        new TopicPartition(pubSubTopicPartition.getPubSubTopic().getName(), pubSubTopicPartition.getPartitionNumber());
    Map<TopicPartition, OffsetAndTimestamp> topicPartitionOffsetMap =
        this.kafkaConsumer.offsetsForTimes(Collections.singletonMap(topicPartition, timestamp));
    if (topicPartitionOffsetMap.isEmpty()) {
      return -1L;
    }
    OffsetAndTimestamp offsetAndTimestamp = topicPartitionOffsetMap.get(topicPartition);
    if (offsetAndTimestamp == null) {
      return null;
    }
    return offsetAndTimestamp.offset();
  }

  @Override
  public Long beginningOffset(PubSubTopicPartition pubSubTopicPartition, Duration timeout) {
    TopicPartition topicPartition =
        new TopicPartition(pubSubTopicPartition.getPubSubTopic().getName(), pubSubTopicPartition.getPartitionNumber());
    Map<TopicPartition, Long> topicPartitionOffset =
        this.kafkaConsumer.beginningOffsets(Collections.singleton(topicPartition), timeout);
    return topicPartitionOffset.get(topicPartition);
  }

  @Override
  public Map<PubSubTopicPartition, Long> endOffsets(Collection<PubSubTopicPartition> partitions, Duration timeout) {
    Map<TopicPartition, PubSubTopicPartition> mapping = buildTopicPartitionMapping(partitions);
    Map<PubSubTopicPartition, Long> pubSubTopicPartitionOffsetMap = new HashMap<>(partitions.size());
    Map<TopicPartition, Long> topicPartitionOffsetMap = this.kafkaConsumer.endOffsets(mapping.keySet(), timeout);
    for (Map.Entry<TopicPartition, Long> entry: topicPartitionOffsetMap.entrySet()) {
      pubSubTopicPartitionOffsetMap.put(mapping.get(entry.getKey()), entry.getValue());
    }
    return pubSubTopicPartitionOffsetMap;
  }

  @Override
  public Long endOffset(PubSubTopicPartition pubSubTopicPartition) {
    TopicPartition topicPartition =
        new TopicPartition(pubSubTopicPartition.getPubSubTopic().getName(), pubSubTopicPartition.getPartitionNumber());
    Map<TopicPartition, Long> topicPartitionOffsetMap =
        this.kafkaConsumer.endOffsets(Collections.singleton(topicPartition));
    return topicPartitionOffsetMap.get(topicPartition);
  }

  private Map<TopicPartition, PubSubTopicPartition> buildTopicPartitionMapping(
      Collection<PubSubTopicPartition> partitions) {
    Map<TopicPartition, PubSubTopicPartition> mapping = new HashMap<>(partitions.size());
    for (PubSubTopicPartition pubSubTopicPartition: partitions) {
      mapping.put(
          new TopicPartition(
              pubSubTopicPartition.getPubSubTopic().getName(),
              pubSubTopicPartition.getPartitionNumber()),
          pubSubTopicPartition);
    }
    return mapping;
  }

  @Override
  public List<PubSubTopicPartitionInfo> partitionsFor(PubSubTopic topic) {
    List<PartitionInfo> partitionInfos = this.kafkaConsumer.partitionsFor(topic.getName());
    if (partitionInfos == null) {
      return null;
    }
    List<PubSubTopicPartitionInfo> pubSubTopicPartitionInfos = new ArrayList<>(partitionInfos.size());
    for (PartitionInfo partitionInfo: partitionInfos) {
      if (partitionInfo.topic().equals(topic.getName())) {
        pubSubTopicPartitionInfos.add(
            new PubSubTopicPartitionInfo(
                topic,
                partitionInfo.partition(),
                partitionInfo.replicas().length,
                partitionInfo.inSyncReplicas().length > 0));
      }
    }
    return pubSubTopicPartitionInfos;
  }

  @Override
  public Map<PubSubTopicPartition, List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>>> poll(long timeoutMs) {
    ConsumerRecords<KafkaKey, KafkaMessageEnvelope> records;
    Map<PubSubTopicPartition, List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>>> polledPubSubMessages =
        new HashMap<>();
    records = kafkaConsumer.poll(Duration.ofMillis(timeoutMs));
    for (TopicPartition topicPartition: records.partitions()) {
      PubSubTopicPartition pubSubTopicPartition = new PubSubTopicPartitionImpl(
          pubSubTopicRepository.getTopic(topicPartition.topic()),
          topicPartition.partition());
      List<ConsumerRecord<KafkaKey, KafkaMessageEnvelope>> topicPartitionConsumerRecords =
          records.records(topicPartition);
      List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>> topicPartitionPubSubMessages =
          new ArrayList<>(topicPartitionConsumerRecords.size());
      for (ConsumerRecord<KafkaKey, KafkaMessageEnvelope> consumerRecord: topicPartitionConsumerRecords) {
        PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> pubSubMessage = new ImmutablePubSubMessage<>(
            consumerRecord.key(),
            consumerRecord.value(),
            pubSubTopicPartition,
            consumerRecord.offset(),
            consumerRecord.timestamp(),
            consumerRecord.serializedKeySize() + consumerRecord.serializedValueSize());
        topicPartitionPubSubMessages.add(pubSubMessage);
      }
      polledPubSubMessages.put(pubSubTopicPartition, topicPartitionPubSubMessages);
    }
    return polledPubSubMessages;
  }

  @Override
  public void assign(Collection<PubSubTopicPartition> pubSubTopicPartitions) {
    Collection<TopicPartition> topicPartitions = pubSubTopicPartitions.stream()
        .map(t -> new TopicPartition(t.getPubSubTopic().getName(), t.getPartitionNumber()))
        .collect(Collectors.toList());
    kafkaConsumer.assign(topicPartitions);
  }

  @Override
  public void seek(PubSubTopicPartition pubSubTopicPartition, long offset) {
    TopicPartition topicPartition =
        new TopicPartition(pubSubTopicPartition.getPubSubTopic().getName(), pubSubTopicPartition.getPartitionNumber());
    kafkaConsumer.seek(topicPartition, offset);
  }

  @Override
  public void close() throws IOException {
    if (this.kafkaAdminClient != null) {
      try {
        this.kafkaAdminClient.close(Duration.ofSeconds(60));
      } catch (Exception e) {
        LOGGER.warn("Exception (suppressed) during kafkaAdminClient.close()", e);
      }
    }
  }

  private Properties marshallProperties(Config config) {
    Properties properties = new Properties();
    /** marshall the configs from {@link ConfigEntry} to {@link Properties} */
    config.entries().forEach(configEntry -> properties.setProperty(configEntry.name(), configEntry.value()));
    return properties;
  }

  private AdminClient getKafkaAdminClient() {
    if (kafkaAdminClient == null) {
      throw new IllegalStateException("initialize(properties) has not been called!");
    }
    return kafkaAdminClient;
  }

  private <T> Map<PubSubTopic, T> getSomethingForAllTopics(Function<Config, T> configTransformer, String content) {
    try {
      Set<PubSubTopic> topicNames = getKafkaAdminClient().listTopics()
          .names()
          .get()
          .stream()
          .map(t -> pubSubTopicRepository.getTopic(t))
          .collect(Collectors.toSet());
      return getSomethingForSomeTopics(topicNames, configTransformer, content);
    } catch (Exception e) {
      throw new VeniceException("Failed to get " + content + " for all topics", e);
    }
  }

  private <T> Map<PubSubTopic, T> getSomethingForSomeTopics(
      Set<PubSubTopic> topicNames,
      Function<Config, T> configTransformer,
      String content) {
    Map<PubSubTopic, T> topicToSomething = new HashMap<>();
    try {
      // Step 1: Marshall topic names into config resources
      Collection<ConfigResource> configResources = topicNames.stream()
          .map(topicName -> new ConfigResource(ConfigResource.Type.TOPIC, topicName.getName()))
          .collect(Collectors.toCollection(ArrayList::new));

      // Step 2: retrieve the configs of specified topics
      getKafkaAdminClient().describeConfigs(configResources)
          .all()
          .get()
          // Step 3: populate the map to be returned
          .forEach(
              (configResource, config) -> topicToSomething.put(
                  pubSubTopicRepository.getTopic(configResource.name()),
                  // Step 4: transform the config
                  configTransformer.apply(config)));

      return topicToSomething;
    } catch (Exception e) {
      int numberOfTopic = topicNames.size();
      String numberOfTopicString = numberOfTopic + " topic" + (numberOfTopic > 1 ? "s" : "");
      throw new VeniceException("Failed to get " + content + " for " + numberOfTopicString, e);
    }
  }
}
