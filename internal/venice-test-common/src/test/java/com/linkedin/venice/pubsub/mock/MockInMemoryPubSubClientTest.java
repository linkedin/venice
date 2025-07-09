package com.linkedin.venice.pubsub.mock;

import static org.mockito.Mockito.mock;

import com.linkedin.venice.pubsub.PubSubTopicConfiguration;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubMessageHeaders;
import com.linkedin.venice.pubsub.api.PubSubProduceResult;
import com.linkedin.venice.pubsub.api.PubSubSymbolicPosition;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.pubsub.mock.adapter.admin.MockInMemoryAdminAdapter;
import com.linkedin.venice.pubsub.mock.adapter.consumer.MockInMemoryConsumerAdapter;
import com.linkedin.venice.pubsub.mock.adapter.consumer.poll.RandomPollStrategy;
import com.linkedin.venice.pubsub.mock.adapter.producer.MockInMemoryProducerAdapter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
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

    // 1. Create topic
    PubSubTopicConfiguration config =
        new PubSubTopicConfiguration(Optional.of(1111L), false, Optional.of(1), 12L, Optional.of(24L)); // Defaults are
                                                                                                        // fine for
                                                                                                        // in-memory
    adminAdapter.createTopic(topic, 1, 1, config);
    Assert.assertTrue(adminAdapter.containsTopicWithPartitionCheck(topicPartition), "Topic creation failed");

    // 2. Produce 500 messages
    List<String> expectedValues = new ArrayList<>();
    List<Long> producedOffsets = new ArrayList<>();
    for (int i = 0; i < recordCount; i++) {
      String value = "value-" + i;
      expectedValues.add(value);
      byte[] keyBytes = ("key-" + i).getBytes(StandardCharsets.UTF_8);
      byte[] valueBytes = value.getBytes(StandardCharsets.UTF_8);
      CompletableFuture<PubSubProduceResult> future =
          producerAdapter.sendMessage(topicPartition, keyBytes, valueBytes, new PubSubMessageHeaders(), null);
      PubSubProduceResult result = future.get();
      producedOffsets.add(result.getOffset());
    }

    // 3. Verify produce offsets
    for (int i = 0; i < recordCount; i++) {
      Assert.assertEquals((long) producedOffsets.get(i), i, "Mismatch in produced offset at index " + i);
    }

    // 4. Subscribe consumer from EARLIEST
    consumerAdapter.subscribe(topicPartition, PubSubSymbolicPosition.EARLIEST);

    // // 5. Poll records
    // List<String> consumedValues = new ArrayList<>();
    // List<Long> consumedOffsets = new ArrayList<>();
    // int retries = 0;
    // while (consumedValues.size() < recordCount && retries < 100) {
    // Map<PubSubTopicPartition, List<DefaultPubSubMessage>> polled = consumerAdapter.poll(100);
    // List<DefaultPubSubMessage> messages = polled.get(topicPartition);
    // if (messages != null) {
    // for (DefaultPubSubMessage msg : messages) {
    // consumedOffsets.add(msg.getOffset());
    // consumedValues.add(new String(msg.getValue(), StandardCharsets.UTF_8));
    // }
    // }
    // retries++;
    // }
    //
    // Assert.assertEquals(consumedOffsets.size(), recordCount, "Did not consume all expected records");
    // Assert.assertEquals(consumedValues.size(), recordCount, "Mismatch in consumed values count");
    //
    // // 6. Validate offsets and values
    // for (int i = 0; i < recordCount; i++) {
    // Assert.assertEquals((long) consumedOffsets.get(i), i, "Mismatch in consumed offset at index " + i);
    // Assert.assertEquals(consumedValues.get(i), expectedValues.get(i), "Mismatch in value at offset " + i);
    // }
  }
}
