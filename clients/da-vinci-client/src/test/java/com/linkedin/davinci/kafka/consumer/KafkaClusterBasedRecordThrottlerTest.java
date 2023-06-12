package com.linkedin.davinci.kafka.consumer;

import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.throttle.EventThrottler;
import com.linkedin.venice.unit.kafka.InMemoryKafkaBroker;
import com.linkedin.venice.utils.TestMockTime;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.testng.Assert;
import org.testng.annotations.Test;


public class KafkaClusterBasedRecordThrottlerTest {
  @Test
  public void testRecordsCanBeThrottledPerRegion() {
    String topic = Utils.getUniqueString("topic");
    InMemoryKafkaBroker inMemoryLocalKafkaBroker = new InMemoryKafkaBroker("local");
    inMemoryLocalKafkaBroker.createTopic(topic, 2);
    InMemoryKafkaBroker inMemoryRemoteKafkaBroker = new InMemoryKafkaBroker("remote");
    inMemoryRemoteKafkaBroker.createTopic(topic, 2);

    AtomicLong remoteKafkaQuota = new AtomicLong(10);

    TestMockTime testTime = new TestMockTime();
    long timeWindowMS = 1000L;
    // Unlimited
    EventThrottler localThrottler =
        new EventThrottler(testTime, -1, timeWindowMS, "local_throttler", true, EventThrottler.REJECT_STRATEGY);

    // Modifiable remote throttler
    EventThrottler remoteThrottler = new EventThrottler(
        testTime,
        remoteKafkaQuota::get,
        timeWindowMS,
        "remote_throttler",
        true,
        EventThrottler.REJECT_STRATEGY);

    Map<String, EventThrottler> kafkaUrlToRecordsThrottler = new HashMap<>();
    kafkaUrlToRecordsThrottler.put(inMemoryLocalKafkaBroker.getKafkaBootstrapServer(), localThrottler);
    kafkaUrlToRecordsThrottler.put(inMemoryRemoteKafkaBroker.getKafkaBootstrapServer(), remoteThrottler);

    KafkaClusterBasedRecordThrottler kafkaClusterBasedRecordThrottler =
        new KafkaClusterBasedRecordThrottler(kafkaUrlToRecordsThrottler);

    Map<PubSubTopicPartition, List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>>> consumerRecords =
        new HashMap<>();
    PubSubTopicPartition pubSubTopicPartition = mock(PubSubTopicPartition.class);
    consumerRecords.put(pubSubTopicPartition, new ArrayList<>());
    for (int i = 0; i < 10; i++) {
      consumerRecords.get(pubSubTopicPartition).add(mock(PubSubMessage.class));
    }

    PubSubConsumerAdapter localConsumer = mock(PubSubConsumerAdapter.class);
    PubSubConsumerAdapter remoteConsumer = mock(PubSubConsumerAdapter.class);

    // Assume consumer.poll always returns some records
    doReturn(consumerRecords).when(localConsumer).poll(anyLong());
    doReturn(consumerRecords).when(remoteConsumer).poll(anyLong());

    // Verify can ingest at least some record from local and remote Kafka
    Map<PubSubTopicPartition, List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>>> localPubSubMessages =
        kafkaClusterBasedRecordThrottler
            .poll(localConsumer, inMemoryLocalKafkaBroker.getKafkaBootstrapServer(), Time.MS_PER_SECOND);
    Map<PubSubTopicPartition, List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>>> remotePubSubMessages =
        kafkaClusterBasedRecordThrottler
            .poll(remoteConsumer, inMemoryRemoteKafkaBroker.getKafkaBootstrapServer(), Time.MS_PER_SECOND);
    Assert.assertSame(localPubSubMessages, consumerRecords);
    Assert.assertSame(remotePubSubMessages, consumerRecords);

    // Pause remote kafka consumption
    remoteKafkaQuota.set(0);

    // Verify does not ingest from remote Kafka
    localPubSubMessages = kafkaClusterBasedRecordThrottler
        .poll(localConsumer, inMemoryLocalKafkaBroker.getKafkaBootstrapServer(), Time.MS_PER_SECOND);
    remotePubSubMessages = kafkaClusterBasedRecordThrottler
        .poll(remoteConsumer, inMemoryRemoteKafkaBroker.getKafkaBootstrapServer(), Time.MS_PER_SECOND);
    Assert.assertSame(localPubSubMessages, consumerRecords);
    Assert.assertNotSame(remotePubSubMessages, consumerRecords);
    Assert.assertTrue(remotePubSubMessages.isEmpty());

    // Resume remote Kafka consumption
    remoteKafkaQuota.set(10);
    testTime.sleep(timeWindowMS); // sleep so throttling window is reset and we don't run into race conditions

    // Verify resumes ingestion from remote Kafka
    localPubSubMessages = kafkaClusterBasedRecordThrottler
        .poll(localConsumer, inMemoryLocalKafkaBroker.getKafkaBootstrapServer(), Time.MS_PER_SECOND);
    remotePubSubMessages = kafkaClusterBasedRecordThrottler
        .poll(remoteConsumer, inMemoryRemoteKafkaBroker.getKafkaBootstrapServer(), Time.MS_PER_SECOND);
    Assert.assertSame(localPubSubMessages, consumerRecords);
    Assert.assertSame(remotePubSubMessages, consumerRecords);
  }
}
