package com.linkedin.venice.replication;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.integration.utils.BrooklinWrapper;
import com.linkedin.venice.integration.utils.KafkaBrokerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.kafka.TopicException;
import com.linkedin.venice.kafka.TopicManager;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.writer.VeniceWriterFactory;
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
import static org.testng.Assert.*;

import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestBrooklin {

  private static final Logger logger = Logger.getLogger(TestBrooklin.class);

  @Test
  public void testGetKafkaURL() {
    String dummyVeniceClusterName = TestUtils.getUniqueString("venice");
    KafkaBrokerWrapper kafka = ServiceFactory.getKafkaBroker();
    String topic = "test-topic";
    BrooklinWrapper brooklin = ServiceFactory.getBrooklinWrapper(kafka);
    TopicManager topicManager =
        new TopicManager(kafka.getZkAddress(), DEFAULT_SESSION_TIMEOUT_MS, DEFAULT_CONNECTION_TIMEOUT_MS,
            DEFAULT_KAFKA_OPERATION_TIMEOUT_MS, 100, 0l, TestUtils.getVeniceConsumerFactory(kafka.getAddress()));

    //enable ssl
    Properties kafkaSslProps = new Properties();
    kafkaSslProps.setProperty(BrooklinTopicReplicator.BROOKLIN_CONNECTION_STRING, brooklin.getBrooklinDmsUri());
    kafkaSslProps.setProperty(ConfigKeys.ENABLE_TOPIC_REPLICATOR_SSL, "true");
    kafkaSslProps.setProperty(TopicReplicator.TOPIC_REPLICATOR_SOURCE_SSL_KAFKA_CLUSTER, kafka.getSSLAddress());
    kafkaSslProps.setProperty(ConfigKeys.CLUSTER_NAME, dummyVeniceClusterName);
    kafkaSslProps.setProperty(BrooklinTopicReplicator.BROOKLIN_CONNECTION_APPLICATION_ID, "venice-test-service");
    kafkaSslProps.setProperty(TopicReplicator.TOPIC_REPLICATOR_SOURCE_KAFKA_CLUSTER, kafka.getAddress());

    BrooklinTopicReplicator replicator =
        new BrooklinTopicReplicator(topicManager, TestUtils.getVeniceTestWriterFactory(kafka.getAddress()), new VeniceProperties(kafkaSslProps));
    Assert.assertEquals(replicator.getKafkaURL(topic), "kafkassl://" + kafka.getSSLAddress() + "/" + topic);

    //disable ssl
    Properties kafkaNonSslProps = new Properties();
    kafkaNonSslProps.putAll(kafkaSslProps);
    kafkaNonSslProps.setProperty(ConfigKeys.ENABLE_TOPIC_REPLICATOR_SSL, "false");

    replicator =
        new BrooklinTopicReplicator(topicManager, TestUtils.getVeniceTestWriterFactory(kafka.getAddress()), new VeniceProperties(kafkaNonSslProps));
    Assert.assertEquals(replicator.getKafkaURL(topic), "kafka://" + kafka.getAddress() + "/" + topic);
  }

  @Test
  public void canReplicateKafkaWithBrooklinTopicReplicator() throws InterruptedException {
    String dummyVeniceClusterName = TestUtils.getUniqueString("venice");
    KafkaBrokerWrapper kafka = ServiceFactory.getKafkaBroker();
    BrooklinWrapper brooklin = ServiceFactory.getBrooklinWrapper(kafka);
    Properties properties = new Properties();
    properties.put(TopicReplicator.TOPIC_REPLICATOR_SOURCE_KAFKA_CLUSTER, kafka.getAddress());
    TopicManager topicManager = new TopicManager(kafka.getZkAddress(), DEFAULT_SESSION_TIMEOUT_MS, DEFAULT_CONNECTION_TIMEOUT_MS, DEFAULT_KAFKA_OPERATION_TIMEOUT_MS,
        100, 0l, TestUtils.getVeniceConsumerFactory(kafka.getAddress()));
    TopicReplicator replicator =
        new BrooklinTopicReplicator(topicManager, TestUtils.getVeniceTestWriterFactory(kafka.getAddress()),
            brooklin.getBrooklinDmsUri(), new VeniceProperties(properties), dummyVeniceClusterName, "venice-test-service", false,
            Optional.empty());

    //Create topics
    int partitionCount = 1;
    String sourceTopic = TestUtils.getUniqueString("source");
    String destinationTopic = TestUtils.getUniqueString("destination");
    topicManager.createTopic(sourceTopic, partitionCount, 1, true);
    topicManager.createTopic(destinationTopic, partitionCount, 1, true);

    byte[] key = TestUtils.getUniqueString("key").getBytes(StandardCharsets.UTF_8);
    byte[] value = TestUtils.getUniqueString("value").getBytes(StandardCharsets.UTF_8);

    //Produce in source topic
    Producer<byte[], byte[]> producer = getKafkaProducer(kafka);
    producer.send(new ProducerRecord<>(sourceTopic, key, value));
    producer.flush();
    producer.close();

    //create replication stream
    try {
      replicator.beginReplication(sourceTopic, destinationTopic, 0);
    } catch (TopicException e) {
      throw new VeniceException(e);
    }

    //check destination topic for records
    String consumeTopic = destinationTopic;
    KafkaConsumer<byte[],byte[]> consumer = getKafkaConsumer(kafka);
    List<TopicPartition> allPartitions = new ArrayList<>();
    for (int p=0;p<consumer.partitionsFor(consumeTopic).size();p++){
      allPartitions.add(new TopicPartition(consumeTopic, p));
    }
    consumer.assign(allPartitions);
    consumer.seekToBeginning(allPartitions);
    List<ConsumerRecord<byte[], byte[]>> buffer = new ArrayList<>();
    long startTime = System.currentTimeMillis();
    while (true) {
      ConsumerRecords<byte[], byte[]> records = consumer.poll(100);
      if (records.isEmpty() && buffer.size() > 3){
        // first 3 records are control messages.
        break;
      }
      if (System.currentTimeMillis() - startTime > TimeUnit.MILLISECONDS.convert(30, TimeUnit.SECONDS)){
        throw new RuntimeException("Test timed out waiting for messages to appear in destination topic after 30 seconds");
      }
      for (ConsumerRecord<byte[], byte[]> record : records) {
        buffer.add(record);
      }
      try {
        Thread.sleep(500);
      } catch (InterruptedException e){
        break;
      }
    }

    assertEquals(buffer.get(3).key(), key);
    assertEquals(buffer.get(3).value(), value);

    try {
      replicator.terminateReplication(sourceTopic, destinationTopic);
    } catch (TopicException e) {
      throw new VeniceException(e);
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
    props.put(ConsumerConfig.GROUP_ID_CONFIG, TestUtils.getUniqueString("test"));
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    KafkaConsumer<byte[],byte[]> consumer = new KafkaConsumer<>(props);
    return consumer;
  }

  @Test
  public void testReflectiveInstantiation() {

    TopicManager topicManager = new TopicManager("some zk connection", DEFAULT_SESSION_TIMEOUT_MS,
        DEFAULT_CONNECTION_TIMEOUT_MS, DEFAULT_KAFKA_OPERATION_TIMEOUT_MS, 100, 0l, TestUtils.getVeniceConsumerFactory("test"));

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
    VeniceWriterFactory veniceWriterFactory = TestUtils.getVeniceTestWriterFactory("some Kafka connection");
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
