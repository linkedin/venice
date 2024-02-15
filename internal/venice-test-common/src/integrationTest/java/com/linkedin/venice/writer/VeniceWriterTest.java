package com.linkedin.venice.writer;

import static com.linkedin.venice.pubsub.PubSubConstants.PUBSUB_OPERATION_TIMEOUT_MS_DEFAULT_VALUE;
import static com.linkedin.venice.utils.Time.MS_PER_SECOND;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.integration.utils.PubSubBrokerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.ProducerMetadata;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.partitioner.DefaultVenicePartitioner;
import com.linkedin.venice.pubsub.PubSubConsumerAdapterFactory;
import com.linkedin.venice.pubsub.PubSubProducerAdapterFactory;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubMessageDeserializer;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.pubsub.manager.TopicManager;
import com.linkedin.venice.serialization.avro.KafkaValueSerializer;
import com.linkedin.venice.serialization.avro.OptimizedKafkaValueSerializer;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.ExceptionUtils;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.pools.LandFillObjectPool;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


@Test
public class VeniceWriterTest {
  private static final Logger LOGGER = LogManager.getLogger(VeniceWriterTest.class);
  private PubSubBrokerWrapper pubSubBrokerWrapper;
  private TopicManager topicManager;
  private PubSubConsumerAdapterFactory pubSubConsumerAdapterFactory;

  private PubSubProducerAdapterFactory pubSubProducerAdapterFactory;

  private final PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();

  @BeforeClass
  public void setUp() {
    pubSubBrokerWrapper = ServiceFactory.getPubSubBroker();
    pubSubConsumerAdapterFactory = pubSubBrokerWrapper.getPubSubClientsFactory().getConsumerAdapterFactory();
    pubSubProducerAdapterFactory = pubSubBrokerWrapper.getPubSubClientsFactory().getProducerAdapterFactory();
    topicManager =
        IntegrationTestPushUtils
            .getTopicManagerRepo(
                PUBSUB_OPERATION_TIMEOUT_MS_DEFAULT_VALUE,
                100L,
                0L,
                pubSubBrokerWrapper,
                pubSubTopicRepository)
            .getLocalTopicManager();
  }

  @AfterClass
  public void cleanUp() throws IOException {
    Utils.closeQuietlyWithErrorLogged(topicManager, pubSubBrokerWrapper);
  }

  private void testThreadSafety(
      int numberOfThreads,
      java.util.function.Consumer<VeniceWriter<KafkaKey, byte[], byte[]>> veniceWriterTask)
      throws ExecutionException, InterruptedException {
    String topicName = TestUtils.getUniqueTopicString("topic-for-vw-thread-safety");
    int partitionCount = 1;
    PubSubTopic pubSubTopic = pubSubTopicRepository.getTopic(topicName);
    topicManager.createTopic(pubSubTopic, partitionCount, 1, true);
    Properties properties = new Properties();
    properties.put(ConfigKeys.KAFKA_BOOTSTRAP_SERVERS, pubSubBrokerWrapper.getAddress());
    properties.put(ConfigKeys.PARTITIONER_CLASS, DefaultVenicePartitioner.class.getName());
    properties.putAll(PubSubBrokerWrapper.getBrokerDetailsForClients(Collections.singletonList(pubSubBrokerWrapper)));

    ExecutorService executorService = null;
    try (VeniceWriter<KafkaKey, byte[], byte[]> veniceWriter =
        TestUtils.getVeniceWriterFactory(properties, pubSubProducerAdapterFactory)
            .createVeniceWriter(
                new VeniceWriterOptions.Builder(topicName).setUseKafkaKeySerializer(true)
                    .setPartitionCount(partitionCount)
                    .build())) {
      executorService = Executors.newFixedThreadPool(numberOfThreads);
      Future[] vwFutures = new Future[numberOfThreads];
      for (int i = 0; i < numberOfThreads; i++) {
        vwFutures[i] = executorService.submit(() -> veniceWriterTask.accept(veniceWriter));
      }
      for (int i = 0; i < numberOfThreads; i++) {
        vwFutures[i].get();
      }
    } finally {
      TestUtils.shutdownExecutor(executorService);
    }
    KafkaValueSerializer kafkaValueSerializer = new OptimizedKafkaValueSerializer();
    PubSubMessageDeserializer pubSubDeserializer = new PubSubMessageDeserializer(
        kafkaValueSerializer,
        new LandFillObjectPool<>(KafkaMessageEnvelope::new),
        new LandFillObjectPool<>(KafkaMessageEnvelope::new));
    try (PubSubConsumerAdapter consumer = pubSubConsumerAdapterFactory
        .create(new VeniceProperties(properties), false, pubSubDeserializer, pubSubBrokerWrapper.getAddress())) {
      PubSubTopicPartition pubSubTopicPartition = new PubSubTopicPartitionImpl(pubSubTopic, 0);
      consumer.subscribe(pubSubTopicPartition, -1);
      int lastSeenSequenceNumber = -1;
      int lastSeenSegmentNumber = -1;
      Map<PubSubTopicPartition, List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>>> messages;
      do {
        messages = consumer.poll(10 * Time.MS_PER_SECOND);
        if (messages.containsKey(pubSubTopicPartition)) {
          for (final PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> message: messages.get(pubSubTopicPartition)) {
            ProducerMetadata producerMetadata = message.getValue().producerMetadata;
            int currentSegmentNumber = producerMetadata.segmentNumber;
            int currentSequenceNumber = producerMetadata.messageSequenceNumber;

            if (currentSegmentNumber == lastSeenSegmentNumber && currentSequenceNumber == lastSeenSequenceNumber + 1) {
              lastSeenSequenceNumber = currentSequenceNumber;
            } else if (currentSegmentNumber == lastSeenSegmentNumber + 1 && currentSequenceNumber == 0) {
              lastSeenSegmentNumber = currentSegmentNumber;
              lastSeenSequenceNumber = currentSequenceNumber;
            } else {
              Assert.fail(
                  "DIV Error caught.\n" + "Last segment Number: " + lastSeenSegmentNumber + ". Current segment number: "
                      + currentSegmentNumber + ".\n" + "Last sequence Number: " + lastSeenSequenceNumber
                      + ". Current sequence number: " + currentSequenceNumber + ".");
            }
          }
        }
      } while (!messages.isEmpty());
    }
  }

  @Test(invocationCount = 3)
  public void testThreadSafetyForPutMessages() throws ExecutionException, InterruptedException {
    testThreadSafety(
        100,
        veniceWriter -> veniceWriter.put(new KafkaKey(MessageType.PUT, "blah".getBytes()), "blah".getBytes(), 1, null));
  }

  /**
   * This test does the following steps:
   * 1. Creates a VeniceWriter with a topic that does not exist.
   * 2. Create a new thread to send a SOS control message to this non-existent topic.
   * 3. The new thread should block on sendMessage() call.
   * 4. Main thread closes the VeniceWriter (no matter 'doFlush' flag is true or false) and
   *    expect the 'sendMessageThread' to unblock.
   */
  @Test(timeOut = 60 * MS_PER_SECOND, dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testVeniceWriterClose(boolean doFlush) {
    String topicName = Utils.getUniqueString("version-topic");
    int partitionCount = 1;

    // Intentionally not create the topic: "version-topic", so that the control message send will also be blocked.
    // topicManager.createTopic(pubSubTopic, partitionCount, 1, true);

    CountDownLatch countDownLatch = new CountDownLatch(1);

    Properties properties = new Properties();
    properties.put(ConfigKeys.KAFKA_BOOTSTRAP_SERVERS, pubSubBrokerWrapper.getAddress());
    properties.put(ConfigKeys.PARTITIONER_CLASS, DefaultVenicePartitioner.class.getName());

    try (VeniceWriter<KafkaKey, byte[], byte[]> veniceWriter =
        TestUtils.getVeniceWriterFactory(properties, pubSubProducerAdapterFactory)
            .createVeniceWriter(
                new VeniceWriterOptions.Builder(topicName).setUseKafkaKeySerializer(true)
                    .setPartitionCount(partitionCount)
                    .build())) {
      ExecutorService executor = Executors.newSingleThreadExecutor();

      Future<?> sendMessageFuture = executor.submit(() -> {
        Thread.currentThread().setName("sendMessageThread");
        countDownLatch.countDown();
        try {
          // send to non-existent topic and block.
          veniceWriter.sendStartOfSegment(0, null);
          fail("sendMessage on non-existent topic should have blocked the executing thread");
        } catch (VeniceException e) {
          LOGGER.info("As expected an exception has been received from sendMessage()", e);
          assertNotNull(e.getMessage(), "Exception thrown by sendMessage does not have a message");
          assertTrue(
              e.getMessage()
                  .contains(
                      String.format(
                          "Got an error while trying to produce message into Kafka. Topic: '%s'",
                          veniceWriter.getTopicName())));
          assertTrue(ExceptionUtils.recursiveMessageContains(e, "Producer closed while send in progress"));
          assertTrue(ExceptionUtils.recursiveMessageContains(e, "Requested metadata update after close"));
          LOGGER.info("All expectations were met in thread: {}", Thread.currentThread().getName());
        }
      });

      try {
        countDownLatch.await();
        // Still wait for some time to make sure blocking sendMessage is inside kafka before closing it.
        Utils.sleep(5000);
        veniceWriter.close(doFlush);

        // this is necessary to check whether expectations in sendMessage thread were met.
        sendMessageFuture.get();
      } catch (Exception e) {
        fail("Producer closing should have succeeded without an exception", e);
      } finally {
        executor.shutdownNow();
      }
    }
  }
}
