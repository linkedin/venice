package com.linkedin.venice.pubsub.adapter.kafka.admin;

import static com.linkedin.venice.stats.KafkaAdminWrapperStats.OCCURRENCE_LATENCY_SENSOR_TYPE.CLOSE;
import static com.linkedin.venice.stats.KafkaAdminWrapperStats.OCCURRENCE_LATENCY_SENSOR_TYPE.CONTAINS_TOPIC;
import static com.linkedin.venice.stats.KafkaAdminWrapperStats.OCCURRENCE_LATENCY_SENSOR_TYPE.CONTAINS_TOPIC_WITH_RETRY;
import static com.linkedin.venice.stats.KafkaAdminWrapperStats.OCCURRENCE_LATENCY_SENSOR_TYPE.CREATE_TOPIC;
import static com.linkedin.venice.stats.KafkaAdminWrapperStats.OCCURRENCE_LATENCY_SENSOR_TYPE.DELETE_TOPIC;
import static com.linkedin.venice.stats.KafkaAdminWrapperStats.OCCURRENCE_LATENCY_SENSOR_TYPE.GET_ALL_TOPIC_RETENTIONS;
import static com.linkedin.venice.stats.KafkaAdminWrapperStats.OCCURRENCE_LATENCY_SENSOR_TYPE.GET_SOME_TOPIC_CONFIGS;
import static com.linkedin.venice.stats.KafkaAdminWrapperStats.OCCURRENCE_LATENCY_SENSOR_TYPE.GET_TOPIC_CONFIG;
import static com.linkedin.venice.stats.KafkaAdminWrapperStats.OCCURRENCE_LATENCY_SENSOR_TYPE.GET_TOPIC_CONFIG_WITH_RETRY;
import static com.linkedin.venice.stats.KafkaAdminWrapperStats.OCCURRENCE_LATENCY_SENSOR_TYPE.LIST_ALL_TOPICS;
import static com.linkedin.venice.stats.KafkaAdminWrapperStats.OCCURRENCE_LATENCY_SENSOR_TYPE.SET_TOPIC_CONFIG;

import com.linkedin.venice.kafka.TopicDoesNotExistException;
import com.linkedin.venice.pubsub.PubSubTopicConfiguration;
import com.linkedin.venice.pubsub.api.PubSubAdminAdapter;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.stats.KafkaAdminWrapperStats;
import com.linkedin.venice.utils.SystemTime;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import io.tehuti.metrics.MetricsRepository;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.function.Supplier;
import javax.annotation.Nonnull;
import org.apache.commons.lang.Validate;


/**
 * This class delegates another {@link PubSubAdminAdapter} instance and keeps track of the invocation rate of methods
 * on the delegated instance
 */
public class InstrumentedApacheKafkaAdminAdapter implements PubSubAdminAdapter {
  private final PubSubAdminAdapter kafkaAdmin;
  private final KafkaAdminWrapperStats kafkaAdminWrapperStats;
  private final Time time;

  public InstrumentedApacheKafkaAdminAdapter(
      PubSubAdminAdapter kafkaAdmin,
      MetricsRepository metricsRepository,
      String statsName) {
    this(kafkaAdmin, metricsRepository, statsName, new SystemTime());
  }

  public InstrumentedApacheKafkaAdminAdapter(
      @Nonnull PubSubAdminAdapter kafkaAdmin,
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
  public void createTopic(
      PubSubTopic topicName,
      int numPartitions,
      int replication,
      PubSubTopicConfiguration pubSubTopicConfiguration) {
    instrument(CREATE_TOPIC, () -> {
      kafkaAdmin.createTopic(topicName, numPartitions, replication, pubSubTopicConfiguration);
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
  public void setTopicConfig(PubSubTopic topicName, PubSubTopicConfiguration pubSubTopicConfiguration) {
    instrument(SET_TOPIC_CONFIG, () -> {
      kafkaAdmin.setTopicConfig(topicName, pubSubTopicConfiguration);
      return null;
    });
  }

  @Override
  public Map<PubSubTopic, Long> getAllTopicRetentions() {
    return instrument(GET_ALL_TOPIC_RETENTIONS, () -> kafkaAdmin.getAllTopicRetentions());
  }

  @Override
  public PubSubTopicConfiguration getTopicConfig(PubSubTopic topicName) throws TopicDoesNotExistException {
    return instrument(GET_TOPIC_CONFIG, () -> kafkaAdmin.getTopicConfig(topicName));
  }

  @Override
  public PubSubTopicConfiguration getTopicConfigWithRetry(PubSubTopic topicName) {
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
  public Map<PubSubTopic, PubSubTopicConfiguration> getSomeTopicConfigs(Set<PubSubTopic> topicNames) {
    return instrument(GET_SOME_TOPIC_CONFIGS, () -> kafkaAdmin.getSomeTopicConfigs(topicNames));
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
    return String
        .format("%s delegated by %s", kafkaAdmin.getClassName(), InstrumentedApacheKafkaAdminAdapter.class.getName());
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
