package com.linkedin.venice.writer;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.integration.utils.KafkaBrokerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.kafka.KafkaClientFactory;
import com.linkedin.venice.kafka.TopicManager;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.ProducerMetadata;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.partitioner.DefaultVenicePartitioner;
import com.linkedin.venice.serialization.KafkaKeySerializer;
import com.linkedin.venice.serialization.avro.KafkaValueSerializer;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


@Test
public class VeniceWriterTest {

  private KafkaBrokerWrapper kafka;
  private TopicManager topicManager;
  private KafkaClientFactory kafkaClientFactory;

  @BeforeClass
  public void setUp() {
    kafka = ServiceFactory.getKafkaBroker();
    kafkaClientFactory = TestUtils.getVeniceConsumerFactory(kafka);
    topicManager = new TopicManager(kafkaClientFactory);
  }

  @AfterClass
  public void tearDown() throws IOException {
    kafka.close();
    topicManager.close();
  }

  private void testThreadSafety(int numberOfThreads, Consumer<VeniceWriter<KafkaKey, byte[], byte[]>> veniceWriterTask) throws ExecutionException, InterruptedException {
    String topicName = TestUtils.getUniqueString("topic-for-vw-thread-safety");
    topicManager.createTopic(topicName, 1, 1, true);
    Properties properties = new Properties();
    properties.put(ConfigKeys.KAFKA_BOOTSTRAP_SERVERS, kafka.getAddress());
    properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, kafka.getAddress());
    properties.put(ConfigKeys.PARTITIONER_CLASS, DefaultVenicePartitioner.class.getName());

    ExecutorService executorService = null;
    try (VeniceWriter<KafkaKey, byte[], byte[]> veniceWriter = new VeniceWriterFactory(properties).createVeniceWriter(topicName)) {
      executorService = Executors.newFixedThreadPool(numberOfThreads);
      Future[] vwFutures = new Future[numberOfThreads];
      for (int i = 0; i < numberOfThreads; i++) {
        vwFutures[i] = executorService.submit(() -> veniceWriterTask.accept(veniceWriter));
      }
      for (int i = 0; i < numberOfThreads; i++) {
        vwFutures[i].get();
      }
    } finally {
      if (executorService != null) {
        executorService.shutdownNow();
      }
    }

    Properties consumerProps = new Properties();
    consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaKeySerializer.class);
    consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaValueSerializer.class);

    try (KafkaConsumer<KafkaKey, KafkaMessageEnvelope> consumer = kafkaClientFactory.getKafkaConsumer(consumerProps)) {
      List<TopicPartition> partitions = Collections.singletonList(new TopicPartition(topicName, 0));
      consumer.assign(partitions);
      consumer.seekToBeginning(partitions);
      int lastSeenSequenceNumber = -1;
      int lastSeenSegmentNumber = -1;
      ConsumerRecords<KafkaKey, KafkaMessageEnvelope> records;
      do {
        records = consumer.poll(10 * Time.MS_PER_SECOND);
        for (final ConsumerRecord<KafkaKey, KafkaMessageEnvelope> record : records) {
          ProducerMetadata producerMetadata = record.value().producerMetadata;
          int currentSegmentNumber = producerMetadata.segmentNumber;
          int currentSequenceNumber = producerMetadata.messageSequenceNumber;

          if (currentSegmentNumber == lastSeenSegmentNumber && currentSequenceNumber == lastSeenSequenceNumber + 1) {
            lastSeenSequenceNumber = currentSequenceNumber;
          } else if (currentSegmentNumber == lastSeenSegmentNumber + 1 && currentSequenceNumber == 0) {
            lastSeenSegmentNumber = currentSegmentNumber;
            lastSeenSequenceNumber = currentSequenceNumber;
          } else {
            Assert.fail("DIV Error caught.\n"
                + "Last segment Number: " + lastSeenSegmentNumber + ". Current segment number: " + currentSegmentNumber + ".\n"
                + "Last sequence Number: " + lastSeenSequenceNumber + ". Current sequence number: " + currentSequenceNumber + ".");
          }
        }
      } while (!records.isEmpty());
    }
  }

  @Test(invocationCount = 3)
  public void testThreadSafetyForPutMessages() throws ExecutionException, InterruptedException {
    testThreadSafety(100, veniceWriter -> veniceWriter.put(
          new KafkaKey(MessageType.PUT, "blah".getBytes()), "blah".getBytes(), 1, null));
  }
}
