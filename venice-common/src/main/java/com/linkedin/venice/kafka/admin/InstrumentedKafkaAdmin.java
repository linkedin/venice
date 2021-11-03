package com.linkedin.venice.kafka.admin;

import com.linkedin.venice.kafka.TopicDoesNotExistException;
import com.linkedin.venice.stats.KafkaAdminWrapperStats;
import com.linkedin.venice.utils.SystemTime;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import io.tehuti.metrics.MetricsRepository;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;


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
      KafkaAdminWrapper kafkaAdmin,
      MetricsRepository metricsRepository,
      String statsName,
      Time time
  ) {
    this.kafkaAdmin = Utils.notNull(kafkaAdmin);
    this.time = Utils.notNull(time);
    this.kafkaAdminWrapperStats = KafkaAdminWrapperStats.getInstance(
        Utils.notNull(metricsRepository),
        Utils.stringNotNullNorEmpty(statsName)
    );
  }

  @Override
  public void initialize(Properties properties) {
    kafkaAdmin.initialize(properties);
  }

  @Override
  public void createTopic(String topicName, int numPartitions, int replication, Properties topicProperties) {
    final long startTimeMs = time.getMilliseconds();
    kafkaAdmin.createTopic(topicName, numPartitions, replication, topicProperties);
    kafkaAdminWrapperStats.recordLatency(
        KafkaAdminWrapperStats.OCCURRENCE_LATENCY_SENSOR_TYPE.CREATE_TOPIC,
        Utils.calculateDurationMs(time, startTimeMs)
    );
  }

  @Override
  public KafkaFuture<Void> deleteTopic(String topicName) {
    final long startTimeMs = time.getMilliseconds();
    KafkaFuture<Void> res = kafkaAdmin.deleteTopic(topicName);
    // This latency measurement is not accurate since this is an async API. But we measure it anyways since we record
    // the occurrence rate at least
    kafkaAdminWrapperStats.recordLatency(
        KafkaAdminWrapperStats.OCCURRENCE_LATENCY_SENSOR_TYPE.DELETE_TOPIC,
        Utils.calculateDurationMs(time, startTimeMs)
    );
    return res;
  }

  @Override
  public Set<String> listAllTopics() {
    final long startTimeMs = time.getMilliseconds();
    Set<String> res = kafkaAdmin.listAllTopics();
    kafkaAdminWrapperStats.recordLatency(
        KafkaAdminWrapperStats.OCCURRENCE_LATENCY_SENSOR_TYPE.LIST_ALL_TOPICS,
        Utils.calculateDurationMs(time, startTimeMs)
    );
    return res;
  }

  @Override
  public void setTopicConfig(String topicName, Properties topicProperties) {
    final long startTimeMs = time.getMilliseconds();
    kafkaAdmin.setTopicConfig(topicName, topicProperties);
    kafkaAdminWrapperStats.recordLatency(
        KafkaAdminWrapperStats.OCCURRENCE_LATENCY_SENSOR_TYPE.SET_TOPIC_CONFIG,
        Utils.calculateDurationMs(time, startTimeMs)
    );
  }

  @Override
  public Map<String, Long> getAllTopicRetentions() {
    final long startTimeMs = time.getMilliseconds();
    final Map<String, Long> res = kafkaAdmin.getAllTopicRetentions();
    kafkaAdminWrapperStats.recordLatency(
        KafkaAdminWrapperStats.OCCURRENCE_LATENCY_SENSOR_TYPE.GET_ALL_TOPIC_RETENTIONS,
        Utils.calculateDurationMs(time, startTimeMs)
    );
    return res;
  }

  @Override
  public Properties getTopicConfig(String topicName) throws TopicDoesNotExistException {
    final long startTimeMs = time.getMilliseconds();
    final Properties res = kafkaAdmin.getTopicConfig(topicName);
    kafkaAdminWrapperStats.recordLatency(
        KafkaAdminWrapperStats.OCCURRENCE_LATENCY_SENSOR_TYPE.GET_TOPIC_CONFIG,
        Utils.calculateDurationMs(time, startTimeMs)
    );
    return res;
  }

  @Override
  public Properties getTopicConfigWithRetry(String topicName) {
    final long startTimeMs = time.getMilliseconds();
    final Properties res = kafkaAdmin.getTopicConfigWithRetry(topicName);
    kafkaAdminWrapperStats.recordLatency(
        KafkaAdminWrapperStats.OCCURRENCE_LATENCY_SENSOR_TYPE.GET_TOPIC_CONFIG_WITH_RETRY,
        Utils.calculateDurationMs(time, startTimeMs)
    );
    return res;
  }

  @Override
  public boolean containsTopic(String topic) {
    final long startTimeMs = time.getMilliseconds();
    final boolean res = kafkaAdmin.containsTopic(topic);
    kafkaAdminWrapperStats.recordLatency(
        KafkaAdminWrapperStats.OCCURRENCE_LATENCY_SENSOR_TYPE.CONTAINS_TOPIC,
        Utils.calculateDurationMs(time, startTimeMs)
    );
    return res;
  }

  @Override
  public boolean containsTopicWithExpectationAndRetry(String topic, int maxRetries, final boolean expectedResult) {
    final long startTimeMs = time.getMilliseconds();
    final boolean res = kafkaAdmin.containsTopicWithExpectationAndRetry(topic, maxRetries, expectedResult);
    kafkaAdminWrapperStats.recordLatency(
        KafkaAdminWrapperStats.OCCURRENCE_LATENCY_SENSOR_TYPE.CONTAINS_TOPIC_WITH_RETRY,
        Utils.calculateDurationMs(time, startTimeMs)
    );
    return res;
  }

  @Override
  public Map<String, Properties> getAllTopicConfig() {
    final long startTimeMs = time.getMilliseconds();
    final Map<String, Properties> res = kafkaAdmin.getAllTopicConfig();
    kafkaAdminWrapperStats.recordLatency(
        KafkaAdminWrapperStats.OCCURRENCE_LATENCY_SENSOR_TYPE.GET_ALL_TOPIC_CONFIG,
        Utils.calculateDurationMs(time, startTimeMs)
    );
    return res;
  }

  @Override
  public boolean isTopicDeletionUnderway() {
    final long startTimeMs = time.getMilliseconds();
    final boolean res = kafkaAdmin.isTopicDeletionUnderway();
    kafkaAdminWrapperStats.recordLatency(
        KafkaAdminWrapperStats.OCCURRENCE_LATENCY_SENSOR_TYPE.IS_TOPIC_DELETION_UNDER_WAY,
        Utils.calculateDurationMs(time, startTimeMs)
    );
    return res;
  }

  @Override
  public void close() throws IOException {
    kafkaAdmin.close();
  }

  @Override
  public String getClassName() {
    return String.format("%s delegated by %s", kafkaAdmin.getClassName(), InstrumentedKafkaAdmin.class.getName());
  }

  @Override
  public Map<String, KafkaFuture<TopicDescription>> describeTopics(Collection<String> topicNames) {
    final long startTimeMs = time.getMilliseconds();
    final Map<String, KafkaFuture<TopicDescription>> res = kafkaAdmin.describeTopics(topicNames);
    kafkaAdminWrapperStats.recordLatency(
        KafkaAdminWrapperStats.OCCURRENCE_LATENCY_SENSOR_TYPE.DESCRIBE_TOPICS,
        Utils.calculateDurationMs(time, startTimeMs)
    );
    return res;
  }
}
