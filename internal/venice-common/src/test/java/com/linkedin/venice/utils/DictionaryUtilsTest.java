package com.linkedin.venice.utils;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.kafka.protocol.ControlMessage;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.kafka.protocol.StartOfPush;
import com.linkedin.venice.kafka.protocol.enums.ControlMessageType;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.ImmutablePubSubMessage;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import java.nio.ByteBuffer;
import java.util.Collections;
import org.testng.Assert;
import org.testng.annotations.Test;


public class DictionaryUtilsTest {
  private final PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();

  private PubSubTopic getTopic() {
    String callingFunction = Thread.currentThread().getStackTrace()[2].getMethodName();
    return pubSubTopicRepository.getTopic(Version.composeKafkaTopic(Utils.getUniqueString(callingFunction), 1));
  }

  @Test
  public void testGetDictionary() {
    PubSubTopic topic = getTopic();
    byte[] dictionaryToSend = "TEST_DICT".getBytes();

    PubSubConsumerAdapter pubSubConsumer = mock(PubSubConsumerAdapter.class);
    PubSubTopicRepository pubSubTopicRepository = mock(PubSubTopicRepository.class);
    PubSubTopicPartition topicPartition = new PubSubTopicPartitionImpl(topic, 0);
    doReturn(topic).when(pubSubTopicRepository).getTopic(topic.getName());

    KafkaKey controlMessageKey = new KafkaKey(MessageType.CONTROL_MESSAGE, new byte[0]);
    StartOfPush startOfPush = new StartOfPush();
    startOfPush.compressionStrategy = CompressionStrategy.ZSTD_WITH_DICT.getValue();
    startOfPush.compressionDictionary = ByteBuffer.wrap(dictionaryToSend);

    ControlMessage sopCM = new ControlMessage();
    sopCM.controlMessageType = ControlMessageType.START_OF_PUSH.getValue();
    sopCM.controlMessageUnion = startOfPush;
    KafkaMessageEnvelope sopWithDictionaryValue =
        new KafkaMessageEnvelope(MessageType.CONTROL_MESSAGE.getValue(), null, sopCM, null);
    PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> sopWithDictionary =
        new ImmutablePubSubMessage<>(controlMessageKey, sopWithDictionaryValue, topicPartition, 0L, 0L, 0);
    doReturn(Collections.singletonMap(topicPartition, Collections.singletonList(sopWithDictionary)))
        .when(pubSubConsumer)
        .poll(anyLong());

    ByteBuffer dictionaryFromKafka =
        DictionaryUtils.readDictionaryFromKafka(topic.getName(), pubSubConsumer, pubSubTopicRepository);
    Assert.assertEquals(dictionaryFromKafka.array(), dictionaryToSend);
    verify(pubSubConsumer, times(1)).subscribe(eq(topicPartition), anyLong());
    verify(pubSubConsumer, times(1)).unSubscribe(topicPartition);
    verify(pubSubConsumer, times(1)).poll(anyLong());
  }

  @Test
  public void testGetDictionaryReturnsNullWhenNoDictionary() {
    PubSubTopic topic = getTopic();

    PubSubConsumerAdapter pubSubConsumer = mock(PubSubConsumerAdapter.class);
    PubSubTopicRepository pubSubTopicRepository = mock(PubSubTopicRepository.class);
    PubSubTopicPartition topicPartition = new PubSubTopicPartitionImpl(topic, 0);
    doReturn(topic).when(pubSubTopicRepository).getTopic(topic.getName());

    KafkaKey controlMessageKey = new KafkaKey(MessageType.CONTROL_MESSAGE, new byte[0]);
    StartOfPush startOfPush = new StartOfPush();

    ControlMessage sopCM = new ControlMessage();
    sopCM.controlMessageType = ControlMessageType.START_OF_PUSH.getValue();
    sopCM.controlMessageUnion = startOfPush;
    KafkaMessageEnvelope sopWithDictionaryValue =
        new KafkaMessageEnvelope(MessageType.CONTROL_MESSAGE.getValue(), null, sopCM, null);
    PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> sopWithDictionary =
        new ImmutablePubSubMessage<>(controlMessageKey, sopWithDictionaryValue, topicPartition, 0L, 0L, 0);
    doReturn(Collections.singletonMap(topicPartition, Collections.singletonList(sopWithDictionary)))
        .when(pubSubConsumer)
        .poll(anyLong());

    ByteBuffer dictionaryFromKafka =
        DictionaryUtils.readDictionaryFromKafka(topic.getName(), pubSubConsumer, pubSubTopicRepository);
    Assert.assertNull(dictionaryFromKafka);
    verify(pubSubConsumer, times(1)).subscribe(eq(topicPartition), anyLong());
    verify(pubSubConsumer, times(1)).unSubscribe(topicPartition);
    verify(pubSubConsumer, times(1)).poll(anyLong());
  }

  @Test
  public void testGetDictionaryReturnsNullWhenNoSOP() {
    PubSubTopic topic = getTopic();

    PubSubConsumerAdapter pubSubConsumer = mock(PubSubConsumerAdapter.class);
    PubSubTopicRepository pubSubTopicRepository = mock(PubSubTopicRepository.class);
    PubSubTopicPartition topicPartition = new PubSubTopicPartitionImpl(topic, 0);
    doReturn(topic).when(pubSubTopicRepository).getTopic(topic.getName());

    KafkaKey dataMessageKey = new KafkaKey(MessageType.PUT, "blah".getBytes());

    Put putMessage = new Put();
    putMessage.putValue = ByteBuffer.wrap("blah".getBytes());
    putMessage.schemaId = 1;
    KafkaMessageEnvelope putMessageValue = new KafkaMessageEnvelope(MessageType.PUT.getValue(), null, putMessage, null);
    PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> sopWithDictionary =
        new ImmutablePubSubMessage<>(dataMessageKey, putMessageValue, topicPartition, 0L, 0L, 0);
    doReturn(Collections.singletonMap(topicPartition, Collections.singletonList(sopWithDictionary)))
        .when(pubSubConsumer)
        .poll(anyLong());

    ByteBuffer dictionaryFromKafka =
        DictionaryUtils.readDictionaryFromKafka(topic.getName(), pubSubConsumer, pubSubTopicRepository);
    Assert.assertNull(dictionaryFromKafka);
    verify(pubSubConsumer, times(1)).subscribe(eq(topicPartition), anyLong());
    verify(pubSubConsumer, times(1)).unSubscribe(topicPartition);
    verify(pubSubConsumer, times(1)).poll(anyLong());
  }

  @Test
  public void testGetDictionaryWaitsTillTopicHasRecords() {
    PubSubTopic topic = getTopic();
    byte[] dictionaryToSend = "TEST_DICT".getBytes();

    PubSubConsumerAdapter pubSubConsumer = mock(PubSubConsumerAdapter.class);
    PubSubTopicRepository pubSubTopicRepository = mock(PubSubTopicRepository.class);
    PubSubTopicPartition topicPartition = new PubSubTopicPartitionImpl(topic, 0);
    doReturn(topic).when(pubSubTopicRepository).getTopic(topic.getName());

    KafkaKey controlMessageKey = new KafkaKey(MessageType.CONTROL_MESSAGE, new byte[0]);
    StartOfPush startOfPush = new StartOfPush();
    startOfPush.compressionStrategy = CompressionStrategy.ZSTD_WITH_DICT.getValue();
    startOfPush.compressionDictionary = ByteBuffer.wrap(dictionaryToSend);

    ControlMessage sopCM = new ControlMessage();
    sopCM.controlMessageType = ControlMessageType.START_OF_PUSH.getValue();
    sopCM.controlMessageUnion = startOfPush;
    KafkaMessageEnvelope sopWithDictionaryValue =
        new KafkaMessageEnvelope(MessageType.CONTROL_MESSAGE.getValue(), null, sopCM, null);
    PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> sopWithDictionary =
        new ImmutablePubSubMessage<>(controlMessageKey, sopWithDictionaryValue, topicPartition, 0L, 0L, 0);
    doReturn(Collections.emptyMap())
        .doReturn(Collections.singletonMap(topicPartition, Collections.singletonList(sopWithDictionary)))
        .when(pubSubConsumer)
        .poll(anyLong());

    ByteBuffer dictionaryFromKafka =
        DictionaryUtils.readDictionaryFromKafka(topic.getName(), pubSubConsumer, pubSubTopicRepository);
    Assert.assertEquals(dictionaryFromKafka.array(), dictionaryToSend);
    verify(pubSubConsumer, times(1)).subscribe(eq(topicPartition), anyLong());
    verify(pubSubConsumer, times(1)).unSubscribe(topicPartition);
    verify(pubSubConsumer, times(2)).poll(anyLong());
  }
}
