package com.linkedin.venice.kafka.admin;

import static com.linkedin.venice.stats.KafkaAdminWrapperStats.OCCURRENCE_LATENCY_SENSOR_TYPE.*;

import com.linkedin.venice.kafka.TopicDoesNotExistException;
import com.linkedin.venice.pubsub.PubSubTopicPartitionInfo;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.stats.KafkaAdminWrapperStats;
import com.linkedin.venice.utils.SystemTime;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import io.tehuti.metrics.MetricsRepository;
import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.function.Supplier;
import javax.annotation.Nonnull;
import org.apache.commons.lang.Validate;


/**
 * This class delegates another {@link KafkaAdminWrapper} instance and keeps track of the invocation rate of methods
 * on the delegated instance
 */
public class InstrumentedKafkaAdmin implements KafkaAdminWrapper {
  private final KafkaAdminWrapper kafkaAdmin;
  private final KafkaAdminWrapperStats kafkaAdminWrapperStats;
  private final Time time;

  public InstrumentedKafkaAdmin(KafkaAdminWrapper kafkaAdmin, MetricsRepository metricsRepository, String statsName) {
    this(kafkaAdmin, metricsRepository, statsName, new SystemTime());
  }

  public InstrumentedKafkaAdmin(
      @Nonnull KafkaAdminWrapper kafkaAdmin,
      @Nonnull MetricsRepository metricsRepository,
      @Nonnull String statsName,
      @Nonnull Time time) {
    Validate.notNull(kafkaAdmin);
    Validate.notNull(metricsRepository);
    Validate.notEmpty(statsName);
    Validate.notNull(time);
    this.kafkaAdmin = kafkaAdmin;
    this.time = time;
    this.kafkaAdminWrapperStats = KafkaAdminWrapperStats.getInstance(metricsRepository, statsName);
  }

  @Override
  public void initialize(Properties properties, PubSubTopicRepository pubSubTopicRepository) {
    kafkaAdmin.initialize(properties, pubSubTopicRepository);
  }

  @Override
  public void createTopic(PubSubTopic topicName, int numPartitions, int replication, Properties topicProperties) {
    instrument(CREATE_TOPIC, () -> {
      kafkaAdmin.createTopic(topicName, numPartitions, replication, topicProperties);
      return null;
    });
  }

  /**
   * Note: This latency measurement is not accurate since this is an async API. But we measure it anyways since
   * we record the occurrence rate at least
   */
  @Override
  public Future<Void> deleteTopic(PubSubTopic topicName) {
    return instrument(DELETE_TOPIC, () -> kafkaAdmin.deleteTopic(topicName));
  }

  @Override
  public Set<PubSubTopic> listAllTopics() {
    return instrument(LIST_ALL_TOPICS, () -> kafkaAdmin.listAllTopics());
  }

  @Override
  public void setTopicConfig(PubSubTopic topicName, Properties topicProperties) {
    instrument(SET_TOPIC_CONFIG, () -> {
      kafkaAdmin.setTopicConfig(topicName, topicProperties);
      return null;
    });
  }

  @Override
  public Map<PubSubTopic, Long> getAllTopicRetentions() {
    return instrument(GET_ALL_TOPIC_RETENTIONS, () -> kafkaAdmin.getAllTopicRetentions());
  }

  @Override
  public Properties getTopicConfig(PubSubTopic topicName) throws TopicDoesNotExistException {
    return instrument(GET_TOPIC_CONFIG, () -> kafkaAdmin.getTopicConfig(topicName));
  }

  @Override
  public Properties getTopicConfigWithRetry(PubSubTopic topicName) {
    return instrument(GET_TOPIC_CONFIG_WITH_RETRY, () -> kafkaAdmin.getTopicConfigWithRetry(topicName));
  }

  @Override
  public boolean containsTopic(PubSubTopic topic) {
    return instrument(CONTAINS_TOPIC, () -> kafkaAdmin.containsTopic(topic));
  }

  @Override
  public boolean containsTopicWithPartitionCheck(PubSubTopicPartition pubSubTopicPartition) {
    return instrument(CONTAINS_TOPIC, () -> kafkaAdmin.containsTopicWithPartitionCheck(pubSubTopicPartition));
  }

  @Override
  public boolean containsTopicWithExpectationAndRetry(PubSubTopic topic, int maxRetries, final boolean expectedResult) {
    return instrument(
        CONTAINS_TOPIC_WITH_RETRY,
        () -> kafkaAdmin.containsTopicWithExpectationAndRetry(topic, maxRetries, expectedResult));
  }

  @Override
  public List<Class<? extends Throwable>> getRetriableExceptions() {
    return kafkaAdmin.getRetriableExceptions();
  }

  @Override
  public Map<PubSubTopic, Properties> getSomeTopicConfigs(Set<PubSubTopic> topicNames) {
    return instrument(GET_SOME_TOPIC_CONFIGS, () -> kafkaAdmin.getSomeTopicConfigs(topicNames));
  }

  @Override
  public boolean isTopicDeletionUnderway() {
    return instrument(IS_TOPIC_DELETION_UNDER_WAY, () -> kafkaAdmin.isTopicDeletionUnderway());
  }

  @Override
  public void close() throws IOException {
    instrument(CLOSE, () -> {
      Utils.closeQuietlyWithErrorLogged(kafkaAdmin);
      return null;
    });
  }

  @Override
  public String getClassName() {
    return String.format("%s delegated by %s", kafkaAdmin.getClassName(), InstrumentedKafkaAdmin.class.getName());
  }

  @Override
  public Long offsetForTime(PubSubTopicPartition pubSubTopicPartition, long timestamp, Duration timeout) {
    return instrument(OFFSET_FOR_TIME, () -> kafkaAdmin.offsetForTime(pubSubTopicPartition, timestamp, timeout));
  }

  @Override
  public Long offsetForTime(PubSubTopicPartition pubSubTopicPartition, long timestamp) {
    return instrument(OFFSET_FOR_TIME, () -> kafkaAdmin.offsetForTime(pubSubTopicPartition, timestamp));
  }

  @Override
  public Long beginningOffset(PubSubTopicPartition pubSubTopicPartition, Duration timeout) {
    return instrument(BEGINNING_OFFSET, () -> kafkaAdmin.beginningOffset(pubSubTopicPartition, timeout));
  }

  @Override
  public Map<PubSubTopicPartition, Long> endOffsets(Collection<PubSubTopicPartition> partitions, Duration timeout) {
    return instrument(END_OFFSETS, () -> kafkaAdmin.endOffsets(partitions, timeout));
  }

  @Override
  public Long endOffset(PubSubTopicPartition partition) {
    return instrument(END_OFFSET, () -> kafkaAdmin.endOffset(partition));
  }

  @Override
  public List<PubSubTopicPartitionInfo> partitionsFor(PubSubTopic topic) {
    return instrument(PARTITIONS_FOR, () -> kafkaAdmin.partitionsFor(topic));
  }

  private <T> T instrument(
      KafkaAdminWrapperStats.OCCURRENCE_LATENCY_SENSOR_TYPE type,
      Supplier<T> functionToInstrument) {
    final long startTimeMs = time.getMilliseconds();
    final T res = functionToInstrument.get();
    kafkaAdminWrapperStats.recordLatency(type, Utils.calculateDurationMs(time, startTimeMs));
    return res;
  }
}
