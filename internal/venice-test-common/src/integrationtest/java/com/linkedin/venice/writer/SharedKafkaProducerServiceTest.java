package com.linkedin.venice.writer;

import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.writer.ApacheKafkaProducer.PROPERTIES_KAFKA_PREFIX;
import static org.apache.kafka.clients.producer.ProducerConfig.BUFFER_MEMORY_CONFIG;
import static org.mockito.Mockito.mock;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.integration.utils.KafkaBrokerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import com.linkedin.venice.kafka.KafkaClientFactory;
import com.linkedin.venice.kafka.TopicManager;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.partitioner.DefaultVenicePartitioner;
import com.linkedin.venice.serialization.DefaultSerializer;
import com.linkedin.venice.serialization.KafkaKeySerializer;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.SystemTime;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import io.tehuti.metrics.MetricsRepository;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class SharedKafkaProducerServiceTest {
  private static final Logger LOGGER = LogManager.getLogger(SharedKafkaProducerServiceTest.class);

  private KafkaBrokerWrapper kafka;
  private TopicManager topicManager;
  private KafkaClientFactory kafkaClientFactory;
  private ZkServerWrapper zkServer;

  @BeforeClass
  private void setUp() {
    zkServer = ServiceFactory.getZkServer();
    kafka = ServiceFactory.getKafkaBroker(zkServer);
    kafkaClientFactory = IntegrationTestPushUtils.getVeniceConsumerFactory(kafka);
    topicManager = new TopicManager(kafkaClientFactory);
  }

  @AfterClass
  private void cleanUp() {
    Utils.closeQuietlyWithErrorLogged(kafka, topicManager, zkServer);
  }

  /**
   * In shared producer mode, verify that one thread is able to produce successfully to a good topic when one thread tries to produce to non-existing topic.
   * @throws Exception
   */
  @Test(timeOut = 60000)
  public void testSharedProducerWithNonExistingTopic() throws Exception {
    String existingTopic = "test-topic-1";
    String nonExistingTopic = "test-topic-2";
    topicManager.createTopic(existingTopic, 1, 1, true);

    SharedKafkaProducerService sharedKafkaProducerService = null;
    try {
      Properties properties = new Properties();
      properties.put(ConfigKeys.KAFKA_BOOTSTRAP_SERVERS, kafka.getAddress());
      properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, kafka.getAddress());
      properties.put(ConfigKeys.PARTITIONER_CLASS, DefaultVenicePartitioner.class.getName());
      properties.put(PROPERTIES_KAFKA_PREFIX + BUFFER_MEMORY_CONFIG, "16384");
      sharedKafkaProducerService = TestUtils.getSharedKafkaProducerService(properties);

      VeniceWriterFactory veniceWriterFactory =
          TestUtils.getVeniceWriterFactoryWithSharedProducer(properties, Optional.of(sharedKafkaProducerService));

      try (VeniceWriter<KafkaKey, byte[], byte[]> veniceWriter1 = veniceWriterFactory.createVeniceWriter(
          existingTopic,
          new KafkaKeySerializer(),
          new DefaultSerializer(),
          new DefaultSerializer(),
          Optional.empty(),
          SystemTime.INSTANCE,
          new DefaultVenicePartitioner(),
          Optional.of(1),
          Optional.empty())) {
        CountDownLatch producedTopicPresent = new CountDownLatch(100);
        for (int i = 0; i < 100 && !Thread.interrupted(); i++) {
          try {
            veniceWriter1
                .put(new KafkaKey(MessageType.PUT, "topic1".getBytes()), "topic1".getBytes(), 1, (metadata, e) -> {
                  if (e != null) {
                    LOGGER.error("Error when producing to an existing topic: {}", existingTopic, e);
                  } else {
                    LOGGER.info("produced offset test-topic-1: {}", metadata.offset());
                    producedTopicPresent.countDown();
                  }
                });
          } catch (VeniceException e) {
            LOGGER.error("Exception: ", e);
          }
        }
        producedTopicPresent.await();
      }

      Thread thread2 = null;
      VeniceWriter<KafkaKey, byte[], byte[]> veniceWriter2 = null;
      try {
        veniceWriter2 = veniceWriterFactory.createVeniceWriter(
            nonExistingTopic,
            new KafkaKeySerializer(),
            new DefaultSerializer(),
            new DefaultSerializer(),
            Optional.empty(),
            SystemTime.INSTANCE,
            new DefaultVenicePartitioner(),
            Optional.of(1),
            Optional.empty());

        AtomicInteger producedTopicNotPresent = new AtomicInteger();
        VeniceWriter<KafkaKey, byte[], byte[]> finalVeniceWriter = veniceWriter2;
        thread2 = new Thread(() -> {
          /**
           * Test would fail if increase messages sent to non-existence topic from 100 to 16384. TODO: Is it expected because
           * buffer is full?
           */
          for (int i = 0; i < 100 && !Thread.interrupted(); i++) {
            try {
              finalVeniceWriter.put(
                  new KafkaKey(MessageType.PUT, "topic2".getBytes()),
                  "topic2".getBytes(),
                  1,
                  (metadata, e) -> producedTopicNotPresent.getAndIncrement());
            } catch (VeniceException e) {
              LOGGER.error("Exception: ", e);
            }
          }
        });
        thread2.start();
        Thread.sleep(1000);
        Assert.assertEquals(producedTopicNotPresent.get(), 0);
      } finally {
        if (thread2 != null) {
          TestUtils.shutdownThread(thread2);
        }
        Utils.closeQuietlyWithErrorLogged(veniceWriter2);

      }
    } finally {
      if (sharedKafkaProducerService != null) {
        sharedKafkaProducerService.stopInner();
      }
    }

    try (Consumer<KafkaKey, KafkaMessageEnvelope> consumer = kafkaClientFactory.getRecordKafkaConsumer()) {
      List<TopicPartition> partitions = Collections.singletonList(new TopicPartition(existingTopic, 0));
      consumer.assign(partitions);
      Long end = consumer.endOffsets(partitions).get(partitions.get(0));
      Assert.assertTrue(end > 100L); // to account for the SOP that VW sends internally.
    }
  }

  private static class ProducerSupplier implements SharedKafkaProducerService.KafkaProducerSupplier {
    @Override
    public KafkaProducerWrapper getNewProducer(VeniceProperties props) {
      return mock(ApacheKafkaProducer.class);
    }
  }

  @Test
  public void testProducerReuse() throws Exception {
    SharedKafkaProducerService sharedKafkaProducerService = null;
    try {
      sharedKafkaProducerService = new SharedKafkaProducerService(
          getProperties(),
          8,
          new ProducerSupplier(),
          new MetricsRepository(),
          Collections.EMPTY_SET);

      // Create at least 8 tasks to assign each producer a task.
      KafkaProducerWrapper producer1 = sharedKafkaProducerService.acquireKafkaProducer("task1");
      KafkaProducerWrapper producer2 = sharedKafkaProducerService.acquireKafkaProducer("task2");
      KafkaProducerWrapper producer3 = sharedKafkaProducerService.acquireKafkaProducer("task3");
      KafkaProducerWrapper producer4 = sharedKafkaProducerService.acquireKafkaProducer("task4");
      KafkaProducerWrapper producer5 = sharedKafkaProducerService.acquireKafkaProducer("task5");
      KafkaProducerWrapper producer6 = sharedKafkaProducerService.acquireKafkaProducer("task6");
      KafkaProducerWrapper producer7 = sharedKafkaProducerService.acquireKafkaProducer("task7");
      KafkaProducerWrapper producer8 = sharedKafkaProducerService.acquireKafkaProducer("task8");

      // verify same task acquires the same producer.
      Assert.assertEquals(producer1, sharedKafkaProducerService.acquireKafkaProducer("task1"));

      // release a producer and verify the last released producer is returned for a new task.
      sharedKafkaProducerService.releaseKafkaProducer("task5");
      Assert.assertEquals(producer5, sharedKafkaProducerService.acquireKafkaProducer("task9"));

      // already released producer should not cause any harm.
      sharedKafkaProducerService.releaseKafkaProducer("task5");
    } finally {
      if (sharedKafkaProducerService != null) {
        sharedKafkaProducerService.stopInner();
      }
    }
  }

  @Test
  public void testProducerClosing() throws Exception {
    SharedKafkaProducerService sharedKafkaProducerService = null;
    try {
      sharedKafkaProducerService = new SharedKafkaProducerService(
          getProperties(),
          8,
          new ProducerSupplier(),
          new MetricsRepository(),
          Collections.EMPTY_SET);
      sharedKafkaProducerService.acquireKafkaProducer("task1");
      sharedKafkaProducerService.releaseKafkaProducer("task1");
    } finally {
      if (sharedKafkaProducerService != null) {
        sharedKafkaProducerService.stopInner();
      }
    }
  }

  private Properties getProperties() {
    Properties properties = new Properties();
    properties.put(KAFKA_BOOTSTRAP_SERVERS, "127.0.0.1:9092");
    return properties;
  }
}
