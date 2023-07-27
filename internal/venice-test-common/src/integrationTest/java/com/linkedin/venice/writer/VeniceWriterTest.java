package com.linkedin.venice.writer;

import static com.linkedin.venice.kafka.TopicManager.DEFAULT_KAFKA_OPERATION_TIMEOUT_MS;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.integration.utils.PubSubBrokerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.kafka.TopicManager;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.ProducerMetadata;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.partitioner.DefaultVenicePartitioner;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapterFactory;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubMessageDeserializer;
import com.linkedin.venice.pubsub.api.PubSubProducerAdapterFactory;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.serialization.avro.KafkaValueSerializer;
import com.linkedin.venice.serialization.avro.OptimizedKafkaValueSerializer;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.pools.LandFillObjectPool;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


@Test
public class VeniceWriterTest {
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
    topicManager = IntegrationTestPushUtils
        .getTopicManagerRepo(DEFAULT_KAFKA_OPERATION_TIMEOUT_MS, 100L, 0L, pubSubBrokerWrapper, pubSubTopicRepository)
        .getTopicManager();
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
}
