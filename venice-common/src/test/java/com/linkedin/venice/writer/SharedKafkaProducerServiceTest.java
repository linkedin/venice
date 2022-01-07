package com.linkedin.venice.writer;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.integration.utils.KafkaBrokerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.kafka.KafkaClientFactory;
import com.linkedin.venice.kafka.TopicManager;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.partitioner.DefaultVenicePartitioner;
import com.linkedin.venice.serialization.DefaultSerializer;
import com.linkedin.venice.serialization.KafkaKeySerializer;
import com.linkedin.venice.utils.SystemTime;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.VeniceProperties;

import io.tehuti.metrics.MetricsRepository;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;

import static com.linkedin.venice.ConfigKeys.*;
import static com.linkedin.venice.writer.ApacheKafkaProducer.*;
import static org.apache.kafka.clients.producer.ProducerConfig.*;
import static org.mockito.Mockito.*;


public class SharedKafkaProducerServiceTest {
  private static final Logger LOGGER = Logger.getLogger(SharedKafkaProducerServiceTest.class);

  private KafkaBrokerWrapper kafka;
  private TopicManager topicManager;
  private KafkaClientFactory kafkaClientFactory;

  private void setUp() {
    kafka = ServiceFactory.getKafkaBroker();
    kafkaClientFactory = TestUtils.getVeniceConsumerFactory(kafka);
    topicManager = new TopicManager(kafkaClientFactory);
  }

  private void cleanUp() throws IOException {
    kafka.close();
    topicManager.close();
  }

  /**
   * In shared producer mode, verify that one thread is able to produce successfully to a good topic when one thread tries to produce to non existing topic.
   * @throws Exception
   */
  @Test (timeOut = 60000)
  public void testSharedProducerWithNonExistingTopic() throws Exception {
    setUp();

    String existingTopic = "test-topic-1";
    String nonExistingTopic = "test-topic-2";

    topicManager.createTopic(existingTopic, 1, 1, true);
    Properties properties = new Properties();
    properties.put(ConfigKeys.KAFKA_BOOTSTRAP_SERVERS, kafka.getAddress());
    properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, kafka.getAddress());
    properties.put(ConfigKeys.PARTITIONER_CLASS, DefaultVenicePartitioner.class.getName());
    properties.put(PROPERTIES_KAFKA_PREFIX + BUFFER_MEMORY_CONFIG, "16384");


    SharedKafkaProducerService sharedKafkaProducerService = TestUtils.getSharedKafkaProducerService(properties);
    VeniceWriterFactory veniceWriterFactory = TestUtils.getVeniceWriterFactoryWithSharedProducer(properties, Optional.of(sharedKafkaProducerService));

    VeniceWriter<KafkaKey, byte[], byte[]> veniceWriter1 = veniceWriterFactory.createVeniceWriter(existingTopic, new KafkaKeySerializer(), new DefaultSerializer(), new DefaultSerializer(),
        Optional.empty(), SystemTime.INSTANCE, new DefaultVenicePartitioner(), Optional.of(1), Optional.empty());
    VeniceWriter<KafkaKey, byte[], byte[]> veniceWriter2 = veniceWriterFactory.createVeniceWriter(nonExistingTopic, new KafkaKeySerializer(), new DefaultSerializer(), new DefaultSerializer(),
        Optional.empty(), SystemTime.INSTANCE, new DefaultVenicePartitioner(), Optional.of(1), Optional.empty());


    AtomicInteger producedTopicPresent = new AtomicInteger();
    AtomicInteger producedTopicNotPresent = new AtomicInteger();

    Thread thread1 = new Thread(() -> {
      for (int i = 0; i < 100; i++) {
        try {
          veniceWriter1.put(new KafkaKey(MessageType.PUT, "topic1".getBytes()), "topic1".getBytes(), 1, new Callback() {
            public void onCompletion(RecordMetadata metadata, Exception e) {
              if(e != null) {
                LOGGER.error("Error when producing to an existing topic " + existingTopic, e);
              } else {
                LOGGER.info("produced offset test-topic-1: " + metadata.offset());
                producedTopicPresent.getAndIncrement();
              }
            }
          });
        } catch (Exception e) {
          LOGGER.error("Exception: ", e);
        }
      }
    });


    Thread thread2 = new Thread(() -> {
      /**
       * Test would fail if increase messages sent to non-existence topic from 100 to 16384. TODO: Is it expected because
       * buffer is full?
       */
      for (int i = 0; i < 100; i++) {
        try {
          veniceWriter2.put(new KafkaKey(MessageType.PUT, "topic2".getBytes()), "topic2".getBytes(), 1, new Callback() {
            public void onCompletion(RecordMetadata metadata, Exception e) {
              if(e != null) {
                LOGGER.error("Error when producing to a non-existing topic " + nonExistingTopic, e);
              } else {
                LOGGER.info("produced offset test-topic-2: " + metadata.offset());
                producedTopicNotPresent.getAndIncrement();
              }
            }
          });
        } catch (Exception e) {
          LOGGER.error("Exception: ", e);
        }
      }
    });

    thread2.start();
    Thread.sleep(1000);
    thread1.start();

    thread1.join();

    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
      Assert.assertEquals(producedTopicPresent.get(), 100);
      Assert.assertEquals(producedTopicNotPresent.get(), 0);
    });

    try (Consumer<KafkaKey, KafkaMessageEnvelope> consumer = kafkaClientFactory.getRecordKafkaConsumer()) {
      List<TopicPartition> partitions = Collections.singletonList(new TopicPartition(existingTopic, 0));
      consumer.assign(partitions);
      Long end = consumer.endOffsets(partitions).get(partitions.get(0));
      Assert.assertTrue(end > 100L); //to account for the SOP that VW sends internally.
    }

    //interrupt thread2 to be able to finish the test.
    thread2.interrupt();
    thread2.join();
    cleanUp();
  }

  public class ProducerSupplier implements SharedKafkaProducerService.KafkaProducerSupplier {
    @Override
    public KafkaProducerWrapper getNewProducer(VeniceProperties props) {
      return mock(ApacheKafkaProducer.class);
    }
  }

  @Test
  public void testProducerReuse() throws Exception {
    //VeniceProperties veniceProperties = AbstractStorageEngineTest.getServerProperties(PersistenceType.ROCKS_DB);
    SharedKafkaProducerService sharedKafkaProducerService =
        new SharedKafkaProducerService(getProperties(), 8, new ProducerSupplier(), new MetricsRepository(),
            Collections.EMPTY_SET);

    //Create at least 8 tasks to assign each producer a task.
    KafkaProducerWrapper producer1 = sharedKafkaProducerService.acquireKafkaProducer("task1");
    KafkaProducerWrapper producer2 = sharedKafkaProducerService.acquireKafkaProducer("task2");
    KafkaProducerWrapper producer3 = sharedKafkaProducerService.acquireKafkaProducer("task3");
    KafkaProducerWrapper producer4 = sharedKafkaProducerService.acquireKafkaProducer("task4");
    KafkaProducerWrapper producer5 = sharedKafkaProducerService.acquireKafkaProducer("task5");
    KafkaProducerWrapper producer6 = sharedKafkaProducerService.acquireKafkaProducer("task6");
    KafkaProducerWrapper producer7 = sharedKafkaProducerService.acquireKafkaProducer("task7");
    KafkaProducerWrapper producer8 = sharedKafkaProducerService.acquireKafkaProducer("task8");

    //verify same task acquires the same producer.
    Assert.assertEquals(producer1, sharedKafkaProducerService.acquireKafkaProducer("task1"));

    //release a producer and verify the last released producer is returned for a new task.
    sharedKafkaProducerService.releaseKafkaProducer("task5");
    Assert.assertEquals(producer5, sharedKafkaProducerService.acquireKafkaProducer("task9"));

    //already released producer should not cause any harm.
    sharedKafkaProducerService.releaseKafkaProducer("task5");

    //close shared producer service.
    sharedKafkaProducerService.stopInner();
  }

  @Test
  public void testProducerClosing() throws Exception {
    SharedKafkaProducerService sharedKafkaProducerService =
        new SharedKafkaProducerService(getProperties(), 8, new ProducerSupplier(), new MetricsRepository(), Collections.EMPTY_SET);

    KafkaProducerWrapper producer1 = sharedKafkaProducerService.acquireKafkaProducer("task1");
    sharedKafkaProducerService.releaseKafkaProducer("task1");
    sharedKafkaProducerService.stopInner();
  }

  private Properties getProperties() {
    Properties properties = new Properties();
    properties.put(KAFKA_BOOTSTRAP_SERVERS, "127.0.0.1:9092");
    return properties;
  }
}
