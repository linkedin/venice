package com.linkedin.venice.writer;

import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.kafka.TopicManager.DEFAULT_KAFKA_OPERATION_TIMEOUT_MS;
import static com.linkedin.venice.pubsub.adapter.kafka.producer.ApacheKafkaProducerConfig.KAFKA_BUFFER_MEMORY;
import static org.mockito.Mockito.mock;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.integration.utils.PubSubBrokerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.kafka.TopicManager;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.partitioner.DefaultVenicePartitioner;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.adapter.kafka.producer.ApacheKafkaProducerAdapter;
import com.linkedin.venice.pubsub.adapter.kafka.producer.ApacheKafkaProducerAdapterFactory;
import com.linkedin.venice.pubsub.adapter.kafka.producer.SharedKafkaProducerAdapterFactory;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapterFactory;
import com.linkedin.venice.pubsub.api.PubSubProducerAdapter;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.pubsub.kafka.KafkaPubSubMessageDeserializer;
import com.linkedin.venice.serialization.avro.KafkaValueSerializer;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.pools.LandFillObjectPool;
import io.tehuti.metrics.MetricsRepository;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class PubSubSharedProducerAdapterFactoryTest {
  private static final Logger LOGGER = LogManager.getLogger(PubSubSharedProducerAdapterFactoryTest.class);
  private PubSubBrokerWrapper pubSubBrokerWrapper;
  private TopicManager topicManager;
  private PubSubConsumerAdapterFactory pubSubConsumerAdapterFactory;
  private PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();

  @BeforeClass
  public void setUp() {
    pubSubBrokerWrapper = ServiceFactory.getPubSubBroker();
    pubSubConsumerAdapterFactory = IntegrationTestPushUtils.getVeniceConsumerFactory();
    topicManager =
        IntegrationTestPushUtils
            .getTopicManagerRepo(
                DEFAULT_KAFKA_OPERATION_TIMEOUT_MS,
                100,
                0L,
                pubSubBrokerWrapper.getAddress(),
                pubSubTopicRepository)
            .getTopicManager();
  }

  @AfterClass
  public void cleanUp() {
    Utils.closeQuietlyWithErrorLogged(topicManager, pubSubBrokerWrapper);
  }

  /**
   * In shared producer mode, verify that one thread is able to produce successfully to a good topic when one thread tries to produce to non-existing topic.
   * @throws Exception
   */
  @Test(timeOut = 60000)
  public void testSharedProducerWithNonExistingTopic() throws Exception {
    PubSubTopic existingTopic = pubSubTopicRepository.getTopic("test-topic-1_v1");
    String nonExistingTopic = "test-topic-2_v1";
    topicManager.createTopic(existingTopic, 1, 1, true);

    SharedKafkaProducerAdapterFactory sharedKafkaProducerAdapterFactory = null;
    Properties properties = new Properties();
    properties.put(ConfigKeys.KAFKA_BOOTSTRAP_SERVERS, pubSubBrokerWrapper.getAddress());
    try {
      properties.put(ConfigKeys.PARTITIONER_CLASS, DefaultVenicePartitioner.class.getName());
      properties.put(KAFKA_BUFFER_MEMORY, "16384");
      sharedKafkaProducerAdapterFactory = TestUtils.getSharedKafkaProducerService(properties);

      VeniceWriterFactory veniceWriterFactory =
          TestUtils.getVeniceWriterFactoryWithSharedProducer(properties, sharedKafkaProducerAdapterFactory);
      try (VeniceWriter<KafkaKey, byte[], byte[]> veniceWriter1 = veniceWriterFactory.createVeniceWriter(
          new VeniceWriterOptions.Builder(existingTopic.getName()).setUseKafkaKeySerializer(true)
              .setPartitionCount(1)
              .build())) {
        CountDownLatch producedTopicPresent = new CountDownLatch(100);
        for (int i = 0; i < 100 && !Thread.interrupted(); i++) {
          try {
            veniceWriter1
                .put(new KafkaKey(MessageType.PUT, "topic1".getBytes()), "topic1".getBytes(), 1, (metadata, e) -> {
                  if (e != null) {
                    LOGGER.error("Error when producing to an existing topic: {}", existingTopic, e);
                  } else {
                    LOGGER.info("produced offset test-topic-1: {}", metadata.getOffset());
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
            new VeniceWriterOptions.Builder(nonExistingTopic).setUseKafkaKeySerializer(true)
                .setPartitionCount(1)
                .build());
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
      if (sharedKafkaProducerAdapterFactory != null) {
        sharedKafkaProducerAdapterFactory.close();
      }
    }
    KafkaPubSubMessageDeserializer pubSubMessageDeserializer = new KafkaPubSubMessageDeserializer(
        new KafkaValueSerializer(),
        new LandFillObjectPool<>(KafkaMessageEnvelope::new),
        new LandFillObjectPool<>(KafkaMessageEnvelope::new));
    try (PubSubConsumerAdapter pubSubConsumer =
        pubSubConsumerAdapterFactory.create(new VeniceProperties(properties), false, pubSubMessageDeserializer, "")) {
      PubSubTopicPartition pubSubTopicPartition = new PubSubTopicPartitionImpl(existingTopic, 0);
      Long end = pubSubConsumer.endOffset(pubSubTopicPartition);
      Assert.assertTrue(end > 100L); // to account for the SOP that VW sends internally.
    }
  }

  @Test
  public void testProducerReuse() throws Exception {
    SharedKafkaProducerAdapterFactory sharedKafkaProducerAdapterFactory = null;
    try {
      sharedKafkaProducerAdapterFactory =
          new SharedKafkaProducerAdapterFactory(getProperties(), 8, new ApacheKafkaProducerAdapterFactory() {
            @Override
            public ApacheKafkaProducerAdapter create(
                VeniceProperties props,
                String producerName,
                String brokerAddressToOverride) {
              return mock(ApacheKafkaProducerAdapter.class);
            }
          }, new MetricsRepository(), Collections.EMPTY_SET);

      // Create at least 8 tasks to assign each producer a task.
      PubSubProducerAdapter producer1 = sharedKafkaProducerAdapterFactory.acquireSharedProducer("task1");
      sharedKafkaProducerAdapterFactory.acquireSharedProducer("task2");
      sharedKafkaProducerAdapterFactory.acquireSharedProducer("task3");
      sharedKafkaProducerAdapterFactory.acquireSharedProducer("task4");
      PubSubProducerAdapter producer5 = sharedKafkaProducerAdapterFactory.acquireSharedProducer("task5");
      sharedKafkaProducerAdapterFactory.acquireSharedProducer("task6");
      sharedKafkaProducerAdapterFactory.acquireSharedProducer("task7");
      sharedKafkaProducerAdapterFactory.acquireSharedProducer("task8");

      // verify same task acquires the same producer.
      Assert.assertEquals(producer1, sharedKafkaProducerAdapterFactory.acquireSharedProducer("task1"));

      // release a producer and verify the last released producer is returned for a new task.
      sharedKafkaProducerAdapterFactory.releaseSharedProducer("task5");
      Assert.assertEquals(producer5, sharedKafkaProducerAdapterFactory.acquireSharedProducer("task9"));

      // already released producer should not cause any harm.
      sharedKafkaProducerAdapterFactory.releaseSharedProducer("task5");
    } finally {
      if (sharedKafkaProducerAdapterFactory != null) {
        sharedKafkaProducerAdapterFactory.close();
      }
    }
  }

  @Test
  public void testProducerClosing() throws Exception {
    SharedKafkaProducerAdapterFactory sharedKafkaProducerAdapterFactory = null;
    try {
      sharedKafkaProducerAdapterFactory =
          new SharedKafkaProducerAdapterFactory(getProperties(), 8, new ApacheKafkaProducerAdapterFactory() {
            @Override
            public ApacheKafkaProducerAdapter create(
                VeniceProperties props,
                String producerName,
                String brokerAddressToOverride) {
              return mock(ApacheKafkaProducerAdapter.class);
            }
          }, new MetricsRepository(), Collections.EMPTY_SET);
      sharedKafkaProducerAdapterFactory.acquireSharedProducer("task1");
      sharedKafkaProducerAdapterFactory.releaseSharedProducer("task1");
    } finally {
      if (sharedKafkaProducerAdapterFactory != null) {
        sharedKafkaProducerAdapterFactory.close();
      }
    }
  }

  private Properties getProperties() {
    Properties properties = new Properties();
    properties.put(KAFKA_BOOTSTRAP_SERVERS, "127.0.0.1:9092");
    return properties;
  }
}
