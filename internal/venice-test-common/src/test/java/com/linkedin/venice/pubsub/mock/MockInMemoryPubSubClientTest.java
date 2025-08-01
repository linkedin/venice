package com.linkedin.venice.pubsub.mock;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.pubsub.PubSubTopicConfiguration;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.DefaultPubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubProduceResult;
import com.linkedin.venice.pubsub.api.PubSubSymbolicPosition;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.pubsub.mock.adapter.admin.MockInMemoryAdminAdapter;
import com.linkedin.venice.pubsub.mock.adapter.consumer.MockInMemoryConsumerAdapter;
import com.linkedin.venice.pubsub.mock.adapter.consumer.poll.RandomPollStrategy;
import com.linkedin.venice.pubsub.mock.adapter.producer.MockInMemoryProducerAdapter;
import com.linkedin.venice.utils.PubSubHelper;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class MockInMemoryPubSubClientTest {
  private static final Logger LOGGER = LogManager.getLogger(MockInMemoryPubSubClientTest.class);
  private static final PubSubTopicRepository TOPIC_REPOSITORY = new PubSubTopicRepository();

  private InMemoryPubSubBroker broker;
  private MockInMemoryAdminAdapter adminAdapter;
  private MockInMemoryProducerAdapter producerAdapter;
  private MockInMemoryConsumerAdapter consumerAdapter;
  private PubSubConsumerAdapter validatingConsumerAdapter;

  @BeforeMethod
  public void setUp() {
    broker = new InMemoryPubSubBroker("Validator");
    adminAdapter = new MockInMemoryAdminAdapter(broker);
    producerAdapter = new MockInMemoryProducerAdapter(broker);
    validatingConsumerAdapter = mock(PubSubConsumerAdapter.class);
    consumerAdapter = new MockInMemoryConsumerAdapter(broker, new RandomPollStrategy(), validatingConsumerAdapter);
    LOGGER.info("Setup complete for {}", MockInMemoryPubSubClientTest.class.getSimpleName());
  }

  @Test
  public void testValidateMessageOrderingAndOffsetContinuityWithSinglePartition() throws Exception {
    int partitionId = 0;
    int recordCount = 500;
    PubSubTopic topic = TOPIC_REPOSITORY.getTopic("test-topic");
    PubSubTopicPartition topicPartition = new PubSubTopicPartitionImpl(topic, partitionId);

    // 1. Create a single-partition topic
    PubSubTopicConfiguration config =
        new PubSubTopicConfiguration(Optional.of(1111L), false, Optional.of(1), 12L, Optional.of(24L));
    adminAdapter.createTopic(topic, 1, 1, config);
    assertTrue(adminAdapter.containsTopicWithPartitionCheck(topicPartition), "Topic creation failed");

    // 2. Produce 500 dummy messages and track their offsets and values
    List<KafkaMessageEnvelope> expectedValues = new ArrayList<>();
    List<Long> producedOffsets = new ArrayList<>();
    for (int i = 0; i < recordCount; i++) {
      KafkaKey key = PubSubHelper.getDummyKey();
      KafkaMessageEnvelope value = PubSubHelper.getDummyValue();
      PubSubProduceResult result =
          producerAdapter.sendMessage(topic.getName(), partitionId, key, value, null, null).get();
      producedOffsets.add(result.getOffset());
      expectedValues.add(value);
    }

    // 3. Validate offsets are contiguous starting from 0
    for (int i = 0; i < recordCount; i++) {
      assertEquals((long) producedOffsets.get(i), i, "Offset mismatch at index " + i);
    }

    // Confirm topic's offset range matches number of produced records
    long positionDiff = consumerAdapter
        .positionDifference(topicPartition, PubSubSymbolicPosition.LATEST, PubSubSymbolicPosition.EARLIEST);
    assertEquals(positionDiff, recordCount, "Offset range mismatch");

    // 4. Subscribe from the earliest offset
    consumerAdapter.subscribe(topicPartition, PubSubSymbolicPosition.EARLIEST);

    // 5. Consume messages and validate offset continuity and lag
    List<KafkaMessageEnvelope> consumedValues = new ArrayList<>();
    List<InMemoryPubSubPosition> consumedOffsets = new ArrayList<>();
    int retries = 0;
    while (consumedValues.size() < recordCount && retries++ < 10000) {
      Map<PubSubTopicPartition, List<DefaultPubSubMessage>> polled = consumerAdapter.poll(100);
      List<DefaultPubSubMessage> messages = polled.get(topicPartition);
      if (messages != null) {
        for (DefaultPubSubMessage msg: messages) {
          InMemoryPubSubPosition position = (InMemoryPubSubPosition) msg.getPosition();
          consumedOffsets.add(position);
          consumedValues.add(msg.getValue());

          // Validate lag from current message to end of topic
          long expectedRemaining = recordCount - consumedValues.size();
          long lagToLatest =
              consumerAdapter.positionDifference(topicPartition, PubSubSymbolicPosition.LATEST, position) - 1;
          assertEquals(
              lagToLatest,
              expectedRemaining,
              "Unexpected lag to latest. Expected: " + expectedRemaining + ", Got: " + lagToLatest);

          // Validate zero lag when comparing position to itself
          long selfLag = consumerAdapter.positionDifference(topicPartition, position, position);
          assertEquals(selfLag, 0, "Expected zero self lag at offset: " + position.getInternalOffset());
        }
      }
    }

    // Ensure all messages were consumed
    assertEquals(consumedOffsets.size(), recordCount, "Not all offsets consumed");
    assertEquals(consumedValues.size(), recordCount, "Not all values consumed");

    // 6. Validate offset continuity and message content integrity
    for (int i = 0; i < recordCount; i++) {
      assertEquals(consumedOffsets.get(i).getInternalOffset(), i, "Offset mismatch at index " + i);
      assertEquals(consumedValues.get(i), expectedValues.get(i), "Value mismatch at offset " + i);
    }
  }
}
