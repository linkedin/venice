package com.linkedin.venice.replication;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.integration.utils.BrooklinWrapper;
import com.linkedin.venice.integration.utils.KafkaBrokerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import com.linkedin.venice.kafka.TopicException;
import com.linkedin.venice.kafka.TopicManager;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.writer.VeniceWriterFactory;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import com.linkedin.venice.utils.VeniceProperties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;

import static com.linkedin.venice.kafka.TopicManager.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestBrooklin {

  private static final Logger logger = Logger.getLogger(TestBrooklin.class);

  @Test
  public void testGetKafkaURL() throws IOException {
    String dummyVeniceClusterName = Utils.getUniqueString("venice");
    String topic = "test-topic";

    try (ZkServerWrapper zkServer = ServiceFactory.getZkServer();
        KafkaBrokerWrapper kafka = ServiceFactory.getKafkaBroker(zkServer);
        BrooklinWrapper brooklin = ServiceFactory.getBrooklinWrapper(kafka);
        TopicManager topicManager =
            new TopicManager(DEFAULT_KAFKA_OPERATION_TIMEOUT_MS, 100, 0l, TestUtils.getVeniceConsumerFactory(kafka));) {
      //enable ssl
      Properties kafkaSslProps = new Properties();
      kafkaSslProps.setProperty(BrooklinTopicReplicator.BROOKLIN_CONNECTION_STRING, brooklin.getBrooklinDmsUri());
      kafkaSslProps.setProperty(ConfigKeys.ENABLE_TOPIC_REPLICATOR_SSL, "true");
      kafkaSslProps.setProperty(TopicReplicator.TOPIC_REPLICATOR_SOURCE_SSL_KAFKA_CLUSTER, kafka.getSSLAddress());
      kafkaSslProps.setProperty(ConfigKeys.CLUSTER_NAME, dummyVeniceClusterName);
      kafkaSslProps.setProperty(BrooklinTopicReplicator.BROOKLIN_CONNECTION_APPLICATION_ID, "venice-test-service");
      kafkaSslProps.setProperty(TopicReplicator.TOPIC_REPLICATOR_SOURCE_KAFKA_CLUSTER, kafka.getAddress());

      BrooklinTopicReplicator replicator =
          new BrooklinTopicReplicator(topicManager, TestUtils.getVeniceWriterFactory(kafka.getAddress()), new VeniceProperties(kafkaSslProps));
      Assert.assertEquals(replicator.getKafkaURL(topic), "kafkassl://" + kafka.getSSLAddress() + "/" + topic);

      //disable ssl
      Properties kafkaNonSslProps = new Properties();
      kafkaNonSslProps.putAll(kafkaSslProps);
      kafkaNonSslProps.setProperty(ConfigKeys.ENABLE_TOPIC_REPLICATOR_SSL, "false");

      replicator =
          new BrooklinTopicReplicator(topicManager, TestUtils.getVeniceWriterFactory(kafka.getAddress()), new VeniceProperties(kafkaNonSslProps));
      Assert.assertEquals(replicator.getKafkaURL(topic), "kafka://" + kafka.getAddress() + "/" + topic);
    }
  }

  private List<ConsumerRecord<byte[], byte[]>> consume(KafkaBrokerWrapper kafka, String topicName, int maxExpectedMessage) {
    try (KafkaConsumer<byte[],byte[]> consumer = getKafkaConsumer(kafka)) {
      List<TopicPartition> allPartitions = new ArrayList<>();
      for (int p = 0; p < consumer.partitionsFor(topicName).size(); p++) {
        allPartitions.add(new TopicPartition(topicName, p));
      }
      consumer.assign(allPartitions);
      consumer.seekToBeginning(allPartitions);
      List<ConsumerRecord<byte[], byte[]>> buffer = new ArrayList<>();
      long startTime = System.currentTimeMillis();
      while (true) {
        ConsumerRecords<byte[], byte[]> records = consumer.poll(100);
        if (records.isEmpty() && buffer.size() > maxExpectedMessage) {
          // first 3 records are control messages.
          break;
        }
        if (System.currentTimeMillis() - startTime > TimeUnit.MILLISECONDS.convert(30, TimeUnit.SECONDS)) {
          Assert.fail(
              "Test timed out waiting for messages to appear in destination topic: " + topicName + " after 30 seconds");
        }
        for (ConsumerRecord<byte[], byte[]> record : records) {
          buffer.add(record);
        }
      }
      return buffer;
    }
  }

  /**
   * This test covers 3 scenarios:
   * 1. Mirroring from an empty source topic.
   * 2. Mirroring from the beginning of a non-empty source topic.
   * 3. Mirroring from the end of a non-empty source topic (skip all the current messages).
   *
   * @throws InterruptedException
   * @throws IOException
   */
  @Test
  public void canReplicateKafkaWithBrooklinTopicReplicator() throws InterruptedException, IOException {
    try (ZkServerWrapper zkServer = ServiceFactory.getZkServer();
        KafkaBrokerWrapper kafka = ServiceFactory.getKafkaBroker(zkServer);
        BrooklinWrapper brooklin = ServiceFactory.getBrooklinWrapper(kafka)) {

      Properties properties = new Properties();
      properties.put(TopicReplicator.TOPIC_REPLICATOR_SOURCE_KAFKA_CLUSTER, kafka.getAddress());
      try (TopicManager topicManager = new TopicManager(DEFAULT_KAFKA_OPERATION_TIMEOUT_MS, 100, 0l, TestUtils.getVeniceConsumerFactory(kafka))) {
        String dummyVeniceClusterName = Utils.getUniqueString("venice");
        TopicReplicator replicator =
            new BrooklinTopicReplicator(topicManager, TestUtils.getVeniceWriterFactory(kafka.getAddress()),
                brooklin.getBrooklinDmsUri(), new VeniceProperties(properties), dummyVeniceClusterName, "venice-test-service", false,
                Optional.empty());

        //Create topics
        int partitionCount = 1;
        String sourceTopic = Utils.getUniqueString("source");
        String destinationTopicForEmptySourceTopic = Utils.getUniqueString("destination");
        topicManager.createTopic(sourceTopic, partitionCount, 1, true);
        topicManager.createTopic(destinationTopicForEmptySourceTopic, partitionCount, 1, true);

        //create replication stream with empty source topic
        try {
          replicator.beginReplication(sourceTopic, destinationTopicForEmptySourceTopic, System.currentTimeMillis(), null);
        } catch (TopicException e) {
          throw new VeniceException(e);
        }

        // Produce a message after setting up the data stream
        byte[] key1 = Utils.getUniqueString("key").getBytes(StandardCharsets.UTF_8);
        byte[] value1 = Utils.getUniqueString("value").getBytes(StandardCharsets.UTF_8);
        try (Producer<byte[], byte[]> producer = getKafkaProducer(kafka)) {
          producer.send(new ProducerRecord<>(sourceTopic, key1, value1));
          producer.flush();
        }
        List<ConsumerRecord<byte[], byte[]>> records = consume(kafka, destinationTopicForEmptySourceTopic, 3);
        assertEquals(records.size(), 4);
        assertEquals(records.get(3).key(), key1);
        assertEquals(records.get(3).value(), value1);


        String destinationTopicWithValidSourceOffset = Utils.getUniqueString("destination");
        topicManager.createTopic(destinationTopicWithValidSourceOffset, partitionCount, 1, true);

        byte[] key2 = Utils.getUniqueString("key").getBytes(StandardCharsets.UTF_8);
        byte[] value2 = Utils.getUniqueString("value").getBytes(StandardCharsets.UTF_8);

        //Produce another message to the source topic
        try (Producer<byte[], byte[]> producer = getKafkaProducer(kafka)) {
          producer.send(new ProducerRecord<>(sourceTopic, key2, value2));
          producer.flush();
        }

        //create replication stream with the valid offset from source topic
        try {
          replicator.beginReplication(sourceTopic, destinationTopicWithValidSourceOffset, 0, null);
        } catch (TopicException e) {
          throw new VeniceException(e);
        }

        //check destination topic for records
        records = consume(kafka, destinationTopicWithValidSourceOffset, 3);
        assertEquals(records.size(), 5);
        assertEquals(records.get(3).key(), key1);
        assertEquals(records.get(3).value(), value1);
        assertEquals(records.get(4).key(), key2);
        assertEquals(records.get(4).value(), value2);

        // Mirroring the source topic with the latest offset
        String destinationTopicWithLatestOffset = Utils.getUniqueString("destination");
        topicManager.createTopic(destinationTopicWithLatestOffset, partitionCount, 1, true);
        // Add some delay
        Utils.sleep(TimeUnit.SECONDS.toMillis(1));
        // Create a new replication stream
        replicator.beginReplication(sourceTopic, destinationTopicWithLatestOffset, System.currentTimeMillis(), null);
        // Produce a new message
        byte[] key3 = Utils.getUniqueString("key").getBytes(StandardCharsets.UTF_8);
        byte[] value3 = Utils.getUniqueString("value").getBytes(StandardCharsets.UTF_8);
        try (Producer<byte[], byte[]> producer = getKafkaProducer(kafka)) {
          producer.send(new ProducerRecord<>(sourceTopic, key3, value3));
          producer.flush();
        }
        // Check whether the another destination topic receives the new message or not
        records = consume(kafka, destinationTopicWithLatestOffset, 3);
        assertEquals(records.size(), 4);
        assertEquals(records.get(3).key(), key3);
        assertEquals(records.get(3).value(), value3);

        try {
          replicator.terminateReplication(sourceTopic, destinationTopicForEmptySourceTopic);
          replicator.terminateReplication(sourceTopic, destinationTopicWithValidSourceOffset);
          replicator.terminateReplication(sourceTopic, destinationTopicWithLatestOffset);
        } catch (TopicException e) {
          throw new VeniceException(e);
        }
      }
    }
  }

  private static Producer<byte[], byte[]> getKafkaProducer(KafkaBrokerWrapper kafka){
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getAddress());
    props.put(ProducerConfig.ACKS_CONFIG, "all");
    props.put(ProducerConfig.RETRIES_CONFIG, 0);
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
    props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
    props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
    Producer<byte[], byte[]> producer = new KafkaProducer<>(props);
    return producer;
  }

  private static KafkaConsumer<byte[],byte[]> getKafkaConsumer(KafkaBrokerWrapper kafka){
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getAddress());
    props.put(ConsumerConfig.GROUP_ID_CONFIG, Utils.getUniqueString("test"));
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    KafkaConsumer<byte[],byte[]> consumer = new KafkaConsumer<>(props);
    return consumer;
  }

  @Test
  public void testReflectiveInstantiation() throws IOException {
    KafkaBrokerWrapper kafka = mock(KafkaBrokerWrapper.class);
    doReturn("kafka_url").when(kafka).getAddress();
    doReturn("kafka_zk").when(kafka).getZkAddress();
    try (TopicManager topicManager = new TopicManager(DEFAULT_KAFKA_OPERATION_TIMEOUT_MS, 100, 0l, TestUtils.getVeniceConsumerFactory(kafka))) {
      // Main case: trying to instantiate the BrooklinTopicReplicator
      String brooklinReplicatorClassName = BrooklinTopicReplicator.class.getName();
      VeniceProperties props = new PropertyBuilder()
          .put(ConfigKeys.ENABLE_TOPIC_REPLICATOR, true)
          .put(ConfigKeys.ENABLE_TOPIC_REPLICATOR_SSL, false)
          .put(TopicReplicator.TOPIC_REPLICATOR_CLASS_NAME, brooklinReplicatorClassName)
          .put(BrooklinTopicReplicator.BROOKLIN_CONNECTION_STRING, "useless...")
          .put(TopicReplicator.TOPIC_REPLICATOR_SOURCE_KAFKA_CLUSTER, "some Kafka connection")
          .put(ConfigKeys.CLUSTER_NAME, "Venice cluster name")
          .put(BrooklinTopicReplicator.BROOKLIN_CONNECTION_APPLICATION_ID, "some app id")
          .build();
      VeniceWriterFactory veniceWriterFactory = TestUtils.getVeniceWriterFactory("some Kafka connection");
      Optional<TopicReplicator> topicReplicator = TopicReplicator.getTopicReplicator(topicManager, props, veniceWriterFactory);

      assertTrue(topicReplicator.isPresent());
      assertEquals(topicReplicator.get().getClass().getName(), brooklinReplicatorClassName);

      // We should fail if no class name is specified
      VeniceProperties badPropsWithNoClassName = new PropertyBuilder()
          .put(ConfigKeys.ENABLE_TOPIC_REPLICATOR, true)
          .build();
      try {
        TopicReplicator.getTopicReplicator(topicManager, badPropsWithNoClassName, veniceWriterFactory);
        fail("TopicReplicator.get() should fail fast if no class name is specified.");
      } catch (Exception e) {
        logger.info("Got an exception, as expected: ", e);
        // Good
      }

      // We should fail if a bad class name is specified
      VeniceProperties badPropsWithBadClassName = new PropertyBuilder()
          .put(ConfigKeys.ENABLE_TOPIC_REPLICATOR, true)
          .put(TopicReplicator.TOPIC_REPLICATOR_CLASS_NAME, "fake.package.name." + brooklinReplicatorClassName)
          .build();
      try {
        TopicReplicator.getTopicReplicator(topicManager, badPropsWithBadClassName, veniceWriterFactory);
        fail("TopicReplicator.get() should fail fast if a bad class name is specified.");
      } catch (Exception e) {
        logger.info("Got an exception, as expected: ", e);
        // Good
      }

      // When specifying the "kill-switch", we should be able to run without a TopicReplicator
      VeniceProperties emptyProps = new PropertyBuilder()
          .put(ConfigKeys.ENABLE_TOPIC_REPLICATOR, false)
          .build();
      Optional<TopicReplicator> emptyTopicReplicator = TopicReplicator.getTopicReplicator(topicManager, emptyProps, veniceWriterFactory);

      assertFalse(emptyTopicReplicator.isPresent());
    }
  }
}
