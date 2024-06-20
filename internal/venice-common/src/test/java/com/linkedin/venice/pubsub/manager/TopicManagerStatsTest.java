package com.linkedin.venice.pubsub.manager;

import static com.linkedin.venice.pubsub.manager.TopicManagerStats.SENSOR_TYPE.CREATE_TOPIC;
import static com.linkedin.venice.pubsub.manager.TopicManagerStats.SENSOR_TYPE.DELETE_TOPIC;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import io.tehuti.Metric;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import java.util.EnumMap;
import java.util.Map;
import org.testng.annotations.Test;


public class TopicManagerStatsTest {
  @Test
  public void testRecordLatencyDoesNotThrowExceptionWhenMetricsAreDisabled() {
    TopicManagerStats stats = new TopicManagerStats(null, "localhost:12345");
    assertNull(stats.getSensorsByTypes());
    stats.recordLatency(CREATE_TOPIC, 100);
    stats.recordLatency(null, 100);
  }

  @Test
  public void testRecordLatency() {
    MetricsRepository metricsRepository = new MetricsRepository();
    String pubSubClusterAddress = "venice.kafka.dc-1.linkedin.com:12345";
    TopicManagerStats stats = new TopicManagerStats(metricsRepository, pubSubClusterAddress);
    EnumMap<TopicManagerStats.SENSOR_TYPE, Sensor> sensorsByTypes = stats.getSensorsByTypes();
    assertNotNull(sensorsByTypes);
    assertEquals(sensorsByTypes.size(), TopicManagerStats.SENSOR_TYPE.values().length);
    assertEquals(stats.getMetricsRepository(), metricsRepository);
    assertEquals(stats.getName(), ".TopicManagerStats_venice_kafka_dc-1_linkedin_com_12345");

    stats.recordLatency(DELETE_TOPIC, 100);
    Map<String, ? extends Metric> metrics = metricsRepository.metrics();
    assertTrue(
        metrics.get(".TopicManagerStats_venice_kafka_dc-1_linkedin_com_12345--delete_topic.OccurrenceRate")
            .value() > 0);

    stats.recordLatency(CREATE_TOPIC, 200);
    assertTrue(
        metrics.get(".TopicManagerStats_venice_kafka_dc-1_linkedin_com_12345--create_topic.OccurrenceRate")
            .value() > 0);
    assertTrue(metrics.get(".TopicManagerStats_venice_kafka_dc-1_linkedin_com_12345--create_topic.Avg").value() > 0);
  }

  @Test
  public void testPubSubFailedAdminOpCount() {
    MetricsRepository metricsRepository = new MetricsRepository();
    String pubSubClusterAddress = "venice.kafka.dc-1.linkedin.com:12345";
    TopicManagerStats stats = new TopicManagerStats(metricsRepository, pubSubClusterAddress);
    assertEquals(stats.getPubSubAdminOpFailureCount(), 0);
    System.out.println(metricsRepository.metrics());
    assertEquals(
        metricsRepository
            .getMetric(".TopicManagerStats_venice_kafka_dc-1_linkedin_com_12345--pub_sub_admin_op_failure_count.Gauge")
            .value(),
        0.0);

    stats.recordPubSubAdminOpFailure();
    stats.recordPubSubAdminOpFailure();
    assertEquals(stats.getPubSubAdminOpFailureCount(), 2);
    assertEquals(
        metricsRepository
            .getMetric(".TopicManagerStats_venice_kafka_dc-1_linkedin_com_12345--pub_sub_admin_op_failure_count.Gauge")
            .value(),
        2.0);

    // should reset the count after reading
    assertEquals(stats.getPubSubAdminOpFailureCount(), 0);
    assertEquals(
        metricsRepository
            .getMetric(".TopicManagerStats_venice_kafka_dc-1_linkedin_com_12345--pub_sub_admin_op_failure_count.Gauge")
            .value(),
        0.0);
  }
}
