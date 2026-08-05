package com.linkedin.venice.spark.input.pubsub;

import static com.linkedin.venice.ConfigKeys.PUBSUB_CONSUMER_ADAPTER_FACTORY_CLASS;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_INPUT_SOURCE_TOPIC_CHUNKING_ENABLED;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_INPUT_TOPIC;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SSL_CONFIGURATOR_CLASS_CONFIG;
import static com.linkedin.venice.vpj.VenicePushJobConstants.VENICE_REPUSH_SOURCE_PUBSUB_BROKER;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.adapter.kafka.common.ApacheKafkaOffsetPosition;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.spark.SparkExecutorTestUtils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.vpj.pubsub.input.PubSubPartitionSplit;
import java.util.Properties;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.testng.annotations.Test;


public class SparkPubSubPartitionReaderFactoryTest {
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testCreateReaderWithInvalidPartitionType() {
    Properties p = new Properties();
    p.setProperty(VENICE_REPUSH_SOURCE_PUBSUB_BROKER, "localhost:9092");
    p.setProperty(KAFKA_INPUT_TOPIC, "test-topic");
    p.setProperty(KAFKA_INPUT_SOURCE_TOPIC_CHUNKING_ENABLED, "false");
    VeniceProperties config = new VeniceProperties(p);

    SparkPubSubPartitionReaderFactory factory = new SparkPubSubPartitionReaderFactory(config);

    InputPartition invalidPartition = new InputPartition() {
    };
    factory.createReader(invalidPartition);
  }

  @Test
  public void testCreateReaderMaterializesExecutorSSL() throws Exception {
    Properties properties = new Properties();
    properties.setProperty(VENICE_REPUSH_SOURCE_PUBSUB_BROKER, "localhost:9092");
    properties.setProperty(KAFKA_INPUT_TOPIC, "test-topic");
    properties.setProperty(KAFKA_INPUT_SOURCE_TOPIC_CHUNKING_ENABLED, "false");
    properties
        .setProperty(SSL_CONFIGURATOR_CLASS_CONFIG, SparkExecutorTestUtils.RecordingSSLConfigurator.class.getName());
    properties.setProperty(
        PUBSUB_CONSUMER_ADAPTER_FACTORY_CLASS,
        SparkExecutorTestUtils.AssertingPubSubConsumerAdapterFactory.class.getName());

    PubSubTopicRepository topicRepository = new PubSubTopicRepository();
    PubSubTopicPartition topicPartition = new PubSubTopicPartitionImpl(topicRepository.getTopic("test-topic"), 0);
    PubSubPosition position = ApacheKafkaOffsetPosition.of(0);
    SparkPubSubInputPartition inputPartition = new SparkPubSubInputPartition(
        new PubSubPartitionSplit(topicRepository, topicPartition, position, position, 0, 0, 0));

    SparkExecutorTestUtils.resetInvocations();
    SparkExecutorTestUtils.withTokenFile(() -> {
      SparkPubSubPartitionReaderFactory factory =
          new SparkPubSubPartitionReaderFactory(new VeniceProperties(properties));
      try (PartitionReader<?> reader = factory.createReader(inputPartition)) {
        assertTrue(reader instanceof SparkPubSubInputPartitionReader);
      }
      assertTrue(SparkExecutorTestUtils.getSslConfiguratorInvocations() > 0);
      assertTrue(SparkExecutorTestUtils.getConsumerFactoryInvocations() > 0);
    });
  }
}
