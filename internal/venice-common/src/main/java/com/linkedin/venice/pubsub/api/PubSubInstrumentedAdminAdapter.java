package com.linkedin.venice.pubsub.api;

import static com.linkedin.venice.stats.PubSubAdminWrapperStats.OCCURRENCE_LATENCY_SENSOR_TYPE.CLOSE;
import static com.linkedin.venice.stats.PubSubAdminWrapperStats.OCCURRENCE_LATENCY_SENSOR_TYPE.CONTAINS_TOPIC;
import static com.linkedin.venice.stats.PubSubAdminWrapperStats.OCCURRENCE_LATENCY_SENSOR_TYPE.CONTAINS_TOPIC_WITH_RETRY;
import static com.linkedin.venice.stats.PubSubAdminWrapperStats.OCCURRENCE_LATENCY_SENSOR_TYPE.CREATE_TOPIC;
import static com.linkedin.venice.stats.PubSubAdminWrapperStats.OCCURRENCE_LATENCY_SENSOR_TYPE.DELETE_TOPIC;
import static com.linkedin.venice.stats.PubSubAdminWrapperStats.OCCURRENCE_LATENCY_SENSOR_TYPE.GET_ALL_TOPIC_RETENTIONS;
import static com.linkedin.venice.stats.PubSubAdminWrapperStats.OCCURRENCE_LATENCY_SENSOR_TYPE.GET_SOME_TOPIC_CONFIGS;
import static com.linkedin.venice.stats.PubSubAdminWrapperStats.OCCURRENCE_LATENCY_SENSOR_TYPE.GET_TOPIC_CONFIG;
import static com.linkedin.venice.stats.PubSubAdminWrapperStats.OCCURRENCE_LATENCY_SENSOR_TYPE.GET_TOPIC_CONFIG_WITH_RETRY;
import static com.linkedin.venice.stats.PubSubAdminWrapperStats.OCCURRENCE_LATENCY_SENSOR_TYPE.LIST_ALL_TOPICS;
import static com.linkedin.venice.stats.PubSubAdminWrapperStats.OCCURRENCE_LATENCY_SENSOR_TYPE.SET_TOPIC_CONFIG;

import com.linkedin.venice.pubsub.PubSubTopicConfiguration;
import com.linkedin.venice.pubsub.api.exceptions.PubSubTopicDoesNotExistException;
import com.linkedin.venice.stats.PubSubAdminWrapperStats;
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
public class PubSubInstrumentedAdminAdapter implements PubSubAdminAdapter {
  private final PubSubAdminAdapter pubSubAdminAdapter;
  private final PubSubAdminWrapperStats pubSubAdminWrapperStats;
  private final Time time;

  public PubSubInstrumentedAdminAdapter(
      PubSubAdminAdapter pubSubAdminAdapter,
      MetricsRepository metricsRepository,
      String statsName) {
    this(pubSubAdminAdapter, metricsRepository, statsName, new SystemTime());
  }

  public PubSubInstrumentedAdminAdapter(
      @Nonnull PubSubAdminAdapter pubSubAdminAdapter,
      @Nonnull MetricsRepository metricsRepository,
      @Nonnull String statsName,
      @Nonnull Time time) {
    Validate.notNull(pubSubAdminAdapter);
    Validate.notNull(metricsRepository);
    Validate.notEmpty(statsName);
    Validate.notNull(time);
    this.pubSubAdminAdapter = pubSubAdminAdapter;
    this.time = time;
    this.pubSubAdminWrapperStats = PubSubAdminWrapperStats.getInstance(metricsRepository, statsName);
  }

  @Override
  public void createTopic(
      PubSubTopic topicName,
      int numPartitions,
      int replication,
      PubSubTopicConfiguration pubSubTopicConfiguration) {
    instrument(CREATE_TOPIC, () -> {
      pubSubAdminAdapter.createTopic(topicName, numPartitions, replication, pubSubTopicConfiguration);
      return null;
    });
  }

  /**
   * Note: This latency measurement is not accurate since this is an async API. But we measure it anyways since
   * we record the occurrence rate at least
   */
  @Override
  public Future<Void> deleteTopic(PubSubTopic topicName) {
    return instrument(DELETE_TOPIC, () -> pubSubAdminAdapter.deleteTopic(topicName));
  }

  @Override
  public Set<PubSubTopic> listAllTopics() {
    return instrument(LIST_ALL_TOPICS, () -> pubSubAdminAdapter.listAllTopics());
  }

  @Override
  public void setTopicConfig(PubSubTopic topicName, PubSubTopicConfiguration pubSubTopicConfiguration) {
    instrument(SET_TOPIC_CONFIG, () -> {
      pubSubAdminAdapter.setTopicConfig(topicName, pubSubTopicConfiguration);
      return null;
    });
  }

  @Override
  public Map<PubSubTopic, Long> getAllTopicRetentions() {
    return instrument(GET_ALL_TOPIC_RETENTIONS, () -> pubSubAdminAdapter.getAllTopicRetentions());
  }

  @Override
  public PubSubTopicConfiguration getTopicConfig(PubSubTopic topicName) throws PubSubTopicDoesNotExistException {
    return instrument(GET_TOPIC_CONFIG, () -> pubSubAdminAdapter.getTopicConfig(topicName));
  }

  @Override
  public PubSubTopicConfiguration getTopicConfigWithRetry(PubSubTopic topicName) {
    return instrument(GET_TOPIC_CONFIG_WITH_RETRY, () -> pubSubAdminAdapter.getTopicConfigWithRetry(topicName));
  }

  @Override
  public boolean containsTopic(PubSubTopic topic) {
    return instrument(CONTAINS_TOPIC, () -> pubSubAdminAdapter.containsTopic(topic));
  }

  @Override
  public boolean containsTopicWithPartitionCheck(PubSubTopicPartition pubSubTopicPartition) {
    return instrument(CONTAINS_TOPIC, () -> pubSubAdminAdapter.containsTopicWithPartitionCheck(pubSubTopicPartition));
  }

  @Override
  public boolean containsTopicWithExpectationAndRetry(PubSubTopic topic, int maxRetries, final boolean expectedResult) {
    return instrument(
        CONTAINS_TOPIC_WITH_RETRY,
        () -> pubSubAdminAdapter.containsTopicWithExpectationAndRetry(topic, maxRetries, expectedResult));
  }

  @Override
  public List<Class<? extends Throwable>> getRetriableExceptions() {
    return pubSubAdminAdapter.getRetriableExceptions();
  }

  @Override
  public Map<PubSubTopic, PubSubTopicConfiguration> getSomeTopicConfigs(Set<PubSubTopic> topicNames) {
    return instrument(GET_SOME_TOPIC_CONFIGS, () -> pubSubAdminAdapter.getSomeTopicConfigs(topicNames));
  }

  @Override
  public void close() throws IOException {
    instrument(CLOSE, () -> {
      Utils.closeQuietlyWithErrorLogged(pubSubAdminAdapter);
      return null;
    });
  }

  @Override
  public String getClassName() {
    return String.format(
        "%s delegated by %s",
        pubSubAdminAdapter.getClassName(),
        PubSubInstrumentedAdminAdapter.class.getName());
  }

  private <T> T instrument(
      PubSubAdminWrapperStats.OCCURRENCE_LATENCY_SENSOR_TYPE type,
      Supplier<T> functionToInstrument) {
    final long startTimeMs = time.getMilliseconds();
    final T res = functionToInstrument.get();
    pubSubAdminWrapperStats.recordLatency(type, Utils.calculateDurationMs(time, startTimeMs));
    return res;
  }
}
