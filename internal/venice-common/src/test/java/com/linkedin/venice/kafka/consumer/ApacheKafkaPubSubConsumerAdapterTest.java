package com.linkedin.venice.kafka.consumer;

import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.linkedin.venice.exceptions.UnsubscribedTopicPartitionException;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.adapter.kafka.KafkaPubSubMessageDeserializer;
import com.linkedin.venice.pubsub.adapter.kafka.consumer.ApacheKafkaConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.VeniceProperties;
import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class ApacheKafkaPubSubConsumerAdapterTest {
  private ApacheKafkaConsumerAdapter apacheKafkaConsumerWithOffsetTrackingDisabled;
  private ApacheKafkaConsumerAdapter apacheKafkaConsumerWithOffsetTrackingEnabled;

  private KafkaConsumer<byte[], byte[]> delegateKafkaConsumer;
  private final PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();

  @BeforeMethod
  public void initConsumer() {
    delegateKafkaConsumer = mock(KafkaConsumer.class);
    Properties properties = new Properties();
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    properties.setProperty(KAFKA_BOOTSTRAP_SERVERS, "broker address");
    KafkaPubSubMessageDeserializer kafkaPubSubMessageDeserializer = mock(KafkaPubSubMessageDeserializer.class);
    apacheKafkaConsumerWithOffsetTrackingDisabled = new ApacheKafkaConsumerAdapter(
        delegateKafkaConsumer,
        new VeniceProperties(properties),
        false,
        kafkaPubSubMessageDeserializer);

    apacheKafkaConsumerWithOffsetTrackingEnabled = new ApacheKafkaConsumerAdapter(
        delegateKafkaConsumer,
        new VeniceProperties(properties),
        true,
        kafkaPubSubMessageDeserializer);
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testApacheKafkaConsumer(boolean enabledOffsetCollection) {
    ApacheKafkaConsumerAdapter consumer = enabledOffsetCollection
        ? apacheKafkaConsumerWithOffsetTrackingEnabled
        : apacheKafkaConsumerWithOffsetTrackingDisabled;
    PubSubTopic testTopic = pubSubTopicRepository.getTopic("test_topic_v1");
    PubSubTopicPartition pubSubTopicPartition = new PubSubTopicPartitionImpl(testTopic, 1);
    TopicPartition topicPartition = new TopicPartition(testTopic.getName(), pubSubTopicPartition.getPartitionNumber());
    Assert.assertThrows(UnsubscribedTopicPartitionException.class, () -> consumer.resetOffset(pubSubTopicPartition));

    // Test subscribe
    consumer.subscribe(pubSubTopicPartition, OffsetRecord.LOWEST_OFFSET);
    verify(delegateKafkaConsumer).assign(Collections.singletonList(topicPartition));
    verify(delegateKafkaConsumer).seekToBeginning(Collections.singletonList(topicPartition));

    // Test assignment check.
    doReturn(Collections.singleton(topicPartition)).when(delegateKafkaConsumer).assignment();
    Assert.assertTrue(consumer.hasAnySubscription());

    // Test pause and resume
    consumer.pause(pubSubTopicPartition);
    verify(delegateKafkaConsumer).pause(Collections.singletonList(topicPartition));
    consumer.resume(pubSubTopicPartition);
    verify(delegateKafkaConsumer).resume(Collections.singletonList(topicPartition));

    // Test reset offset
    consumer.resetOffset(pubSubTopicPartition);
    verify(delegateKafkaConsumer, times(2)).seekToBeginning(Collections.singletonList(topicPartition));

    // Test unsubscribe
    consumer.unSubscribe(pubSubTopicPartition);
    verify(delegateKafkaConsumer).assign(Collections.EMPTY_LIST);

    // Test subscribe with not seek beginning.
    int lastReadOffset = 0;
    doReturn(Collections.EMPTY_SET).when(delegateKafkaConsumer).assignment();
    Assert.assertFalse(consumer.hasAnySubscription());
    consumer.subscribe(pubSubTopicPartition, lastReadOffset);
    verify(delegateKafkaConsumer).seek(topicPartition, lastReadOffset + 1);

    // Test batch unsubscribe.
    Set<PubSubTopicPartition> pubSubTopicPartitionsToUnSub = new HashSet<>();
    Set<TopicPartition> topicPartitionsLeft = new HashSet<>();
    Set<PubSubTopicPartition> allPubSubTopicPartitions = new HashSet<>();
    Set<TopicPartition> allTopicPartitions = new HashSet<>();
    PubSubTopic testTopicV2 = pubSubTopicRepository.getTopic("test_topic_v2");
    for (int i = 0; i < 5; i++) {
      PubSubTopicPartition pubSubTopicPartitionToSub = new PubSubTopicPartitionImpl(testTopic, i);
      pubSubTopicPartitionsToUnSub.add(pubSubTopicPartitionToSub);
      allTopicPartitions.add(new TopicPartition(testTopic.getName(), i));
      allPubSubTopicPartitions.add(pubSubTopicPartitionToSub);
      consumer.subscribe(pubSubTopicPartitionToSub, -1);
    }
    for (int i = 0; i < 3; i++) {
      PubSubTopicPartition pubSubTopicPartitionToUnSub = new PubSubTopicPartitionImpl(testTopicV2, i);
      TopicPartition topicPartitionLeft = new TopicPartition(testTopicV2.getName(), i);
      topicPartitionsLeft.add(topicPartitionLeft);
      allTopicPartitions.add(topicPartitionLeft);
      allPubSubTopicPartitions.add(pubSubTopicPartitionToUnSub);
      consumer.subscribe(pubSubTopicPartitionToUnSub, -1);
    }
    doReturn(allTopicPartitions).when(delegateKafkaConsumer).assignment();
    Assert.assertTrue(consumer.hasSubscription(pubSubTopicPartition));
    Assert.assertEquals(consumer.getAssignment(), allPubSubTopicPartitions);
    consumer.batchUnsubscribe(pubSubTopicPartitionsToUnSub);
    verify(delegateKafkaConsumer).assign(topicPartitionsLeft);

    // Test close
    consumer.close();
    verify(delegateKafkaConsumer).close(eq(Duration.ZERO));
  }

}
