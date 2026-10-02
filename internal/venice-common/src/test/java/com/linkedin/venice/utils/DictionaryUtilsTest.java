package com.linkedin.venice.utils;

import static com.linkedin.venice.ConfigKeys.PUBSUB_BROKER_ADDRESS;
import static com.linkedin.venice.ConfigKeys.PUBSUB_CONSUMER_ADAPTER_FACTORY_CLASS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
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
import com.linkedin.venice.pubsub.PubSubConsumerAdapterContext;
import com.linkedin.venice.pubsub.PubSubConsumerAdapterFactory;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.adapter.kafka.common.ApacheKafkaOffsetPosition;
import com.linkedin.venice.pubsub.api.DefaultPubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
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
    PubSubTopicPartition topicPartition = new PubSubTopicPartitionImpl(topic, 0);

    KafkaKey controlMessageKey = new KafkaKey(MessageType.CONTROL_MESSAGE, new byte[0]);
    StartOfPush startOfPush = new StartOfPush();
    startOfPush.compressionStrategy = CompressionStrategy.ZSTD_WITH_DICT.getValue();
    startOfPush.compressionDictionary = ByteBuffer.wrap(dictionaryToSend);

    ControlMessage sopCM = new ControlMessage();
    sopCM.controlMessageType = ControlMessageType.START_OF_PUSH.getValue();
    sopCM.controlMessageUnion = startOfPush;
    KafkaMessageEnvelope sopWithDictionaryValue =
        new KafkaMessageEnvelope(MessageType.CONTROL_MESSAGE.getValue(), null, sopCM, null);
    DefaultPubSubMessage sopWithDictionary = new ImmutablePubSubMessage(
        controlMessageKey,
        sopWithDictionaryValue,
        topicPartition,
        ApacheKafkaOffsetPosition.of(0),
        0L,
        0);
    doReturn(Collections.singletonMap(topicPartition, Collections.singletonList(sopWithDictionary)))
        .when(pubSubConsumer)
        .poll(anyLong());

    ByteBuffer dictionaryFromKafka =
        DictionaryUtils.readDictionaryFromKafka(topic.getName(), pubSubConsumer, pubSubTopicRepository);
    Assert.assertEquals(dictionaryFromKafka.array(), dictionaryToSend);
    verify(pubSubConsumer, times(1)).subscribe(eq(topicPartition), any(PubSubPosition.class));
    verify(pubSubConsumer, times(1)).unSubscribe(topicPartition);
    verify(pubSubConsumer, times(1)).poll(anyLong());
  }

  @Test
  public void testGetDictionaryReturnsNullWhenNoDictionary() {
    PubSubTopic topic = getTopic();

    PubSubConsumerAdapter pubSubConsumer = mock(PubSubConsumerAdapter.class);
    PubSubTopicPartition topicPartition = new PubSubTopicPartitionImpl(topic, 0);

    KafkaKey controlMessageKey = new KafkaKey(MessageType.CONTROL_MESSAGE, new byte[0]);
    StartOfPush startOfPush = new StartOfPush();

    ControlMessage sopCM = new ControlMessage();
    sopCM.controlMessageType = ControlMessageType.START_OF_PUSH.getValue();
    sopCM.controlMessageUnion = startOfPush;
    KafkaMessageEnvelope sopWithDictionaryValue =
        new KafkaMessageEnvelope(MessageType.CONTROL_MESSAGE.getValue(), null, sopCM, null);
    DefaultPubSubMessage sopWithDictionary = new ImmutablePubSubMessage(
        controlMessageKey,
        sopWithDictionaryValue,
        topicPartition,
        ApacheKafkaOffsetPosition.of(0),
        0L,
        0);
    doReturn(Collections.singletonMap(topicPartition, Collections.singletonList(sopWithDictionary)))
        .when(pubSubConsumer)
        .poll(anyLong());

    ByteBuffer dictionaryFromKafka =
        DictionaryUtils.readDictionaryFromKafka(topic.getName(), pubSubConsumer, pubSubTopicRepository);
    Assert.assertNull(dictionaryFromKafka);
    verify(pubSubConsumer, times(1)).subscribe(eq(topicPartition), any(PubSubPosition.class));
    verify(pubSubConsumer, times(1)).unSubscribe(topicPartition);
    verify(pubSubConsumer, times(1)).poll(anyLong());
  }

  @Test
  public void testGetDictionaryReturnsNullWhenNoSOP() {
    PubSubTopic topic = getTopic();

    PubSubConsumerAdapter pubSubConsumer = mock(PubSubConsumerAdapter.class);
    PubSubTopicPartition topicPartition = new PubSubTopicPartitionImpl(topic, 0);

    KafkaKey dataMessageKey = new KafkaKey(MessageType.PUT, "blah".getBytes());

    Put putMessage = new Put();
    putMessage.putValue = ByteBuffer.wrap("blah".getBytes());
    putMessage.schemaId = 1;
    KafkaMessageEnvelope putMessageValue = new KafkaMessageEnvelope(MessageType.PUT.getValue(), null, putMessage, null);
    DefaultPubSubMessage sopWithDictionary = new ImmutablePubSubMessage(
        dataMessageKey,
        putMessageValue,
        topicPartition,
        ApacheKafkaOffsetPosition.of(0),
        0L,
        0);
    doReturn(Collections.singletonMap(topicPartition, Collections.singletonList(sopWithDictionary)))
        .when(pubSubConsumer)
        .poll(anyLong());

    ByteBuffer dictionaryFromKafka =
        DictionaryUtils.readDictionaryFromKafka(topic.getName(), pubSubConsumer, pubSubTopicRepository);
    Assert.assertNull(dictionaryFromKafka);
    verify(pubSubConsumer, times(1)).subscribe(eq(topicPartition), any(PubSubPosition.class));
    verify(pubSubConsumer, times(1)).unSubscribe(topicPartition);
    verify(pubSubConsumer, times(1)).poll(anyLong());
  }

  @Test
  public void testGetDictionaryWaitsTillTopicHasRecords() {
    PubSubTopic topic = getTopic();
    byte[] dictionaryToSend = "TEST_DICT".getBytes();

    PubSubConsumerAdapter pubSubConsumer = mock(PubSubConsumerAdapter.class);
    PubSubTopicPartition topicPartition = new PubSubTopicPartitionImpl(topic, 0);

    KafkaKey controlMessageKey = new KafkaKey(MessageType.CONTROL_MESSAGE, new byte[0]);
    StartOfPush startOfPush = new StartOfPush();
    startOfPush.compressionStrategy = CompressionStrategy.ZSTD_WITH_DICT.getValue();
    startOfPush.compressionDictionary = ByteBuffer.wrap(dictionaryToSend);

    ControlMessage sopCM = new ControlMessage();
    sopCM.controlMessageType = ControlMessageType.START_OF_PUSH.getValue();
    sopCM.controlMessageUnion = startOfPush;
    KafkaMessageEnvelope sopWithDictionaryValue =
        new KafkaMessageEnvelope(MessageType.CONTROL_MESSAGE.getValue(), null, sopCM, null);
    DefaultPubSubMessage sopWithDictionary = new ImmutablePubSubMessage(
        controlMessageKey,
        sopWithDictionaryValue,
        topicPartition,
        ApacheKafkaOffsetPosition.of(0),
        0L,
        0);
    doReturn(Collections.emptyMap())
        .doReturn(Collections.singletonMap(topicPartition, Collections.singletonList(sopWithDictionary)))
        .when(pubSubConsumer)
        .poll(anyLong());

    ByteBuffer dictionaryFromKafka =
        DictionaryUtils.readDictionaryFromKafka(topic.getName(), pubSubConsumer, pubSubTopicRepository);
    Assert.assertNotNull(dictionaryFromKafka);
    Assert.assertEquals(dictionaryFromKafka.array(), dictionaryToSend);
    verify(pubSubConsumer, times(1)).subscribe(eq(topicPartition), any(PubSubPosition.class));
    verify(pubSubConsumer, times(1)).unSubscribe(topicPartition);
    verify(pubSubConsumer, times(2)).poll(anyLong());
  }

  /**
   * Regression test for the V2 (li-crypt) dictionary decryption bug: readDictionaryFromKafkaWithEncryptionLookup
   * previously had no direct coverage of its own (it was only exercised indirectly through callers like
   * KafkaInputUtils#getCompressor), so a future change to a caller's call pattern could silently drop coverage
   * of this wiring. Verifies the lookup is set on the PubSubConsumerAdapterContext the dictionary consumer is
   * built with.
   */
  @Test
  public void testReadDictionaryFromKafkaWithEncryptionLookupPassesLookupToConsumerContext() {
    RecordingPubSubConsumerAdapterFactory.reset();
    Function<String, String> encryptionKeyUrnLookup = storeName -> "urn:li:dataEncryptionKey:test-key";

    Properties props = new Properties();
    props.setProperty(PUBSUB_BROKER_ADDRESS, "localhost:9092");
    props.setProperty(PUBSUB_CONSUMER_ADAPTER_FACTORY_CLASS, RecordingPubSubConsumerAdapterFactory.class.getName());

    ByteBuffer dictionaryFromKafka = DictionaryUtils.readDictionaryFromKafkaWithEncryptionLookup(
        getTopic().getName(),
        new VeniceProperties(props),
        encryptionKeyUrnLookup);

    Assert.assertEquals(dictionaryFromKafka.array(), RecordingPubSubConsumerAdapterFactory.DICTIONARY_TO_SERVE);
    Function<String, String> observedLookup = RecordingPubSubConsumerAdapterFactory.getObservedEncryptionKeyUrnLookup();
    Assert.assertNotNull(observedLookup, "Consumer context should carry the configured pubSubEncryptionKeyUrnLookup");
    Assert.assertEquals(observedLookup.apply("any-store"), "urn:li:dataEncryptionKey:test-key");
  }

  /**
   * Companion to the test above: confirms non-encrypted (the common case) dictionary reads are unaffected —
   * when no lookup is passed in, the consumer context's lookup stays null, same as before the fix.
   */
  @Test
  public void testReadDictionaryFromKafkaWithEncryptionLookupLeavesContextLookupNullWhenNotProvided() {
    RecordingPubSubConsumerAdapterFactory.reset();

    Properties props = new Properties();
    props.setProperty(PUBSUB_BROKER_ADDRESS, "localhost:9092");
    props.setProperty(PUBSUB_CONSUMER_ADAPTER_FACTORY_CLASS, RecordingPubSubConsumerAdapterFactory.class.getName());

    ByteBuffer dictionaryFromKafka = DictionaryUtils
        .readDictionaryFromKafkaWithEncryptionLookup(getTopic().getName(), new VeniceProperties(props), null);

    Assert.assertEquals(dictionaryFromKafka.array(), RecordingPubSubConsumerAdapterFactory.DICTIONARY_TO_SERVE);
    Assert.assertNull(RecordingPubSubConsumerAdapterFactory.getObservedEncryptionKeyUrnLookup());
  }

  /**
   * Test double for {@link PubSubConsumerAdapterFactory} that records the pubSubEncryptionKeyUrnLookup it's
   * constructed with and returns a mock consumer serving a minimal StartOfPush control message, so
   * readDictionaryFromKafkaWithEncryptionLookup's read succeeds without needing a real Kafka broker.
   */
  public static class RecordingPubSubConsumerAdapterFactory
      extends PubSubConsumerAdapterFactory<PubSubConsumerAdapter> {
    static final byte[] DICTIONARY_TO_SERVE = "TEST_DICT".getBytes();
    private static final AtomicReference<Function<String, String>> OBSERVED_ENCRYPTION_KEY_URN_LOOKUP =
        new AtomicReference<>();

    static void reset() {
      OBSERVED_ENCRYPTION_KEY_URN_LOOKUP.set(null);
    }

    static Function<String, String> getObservedEncryptionKeyUrnLookup() {
      return OBSERVED_ENCRYPTION_KEY_URN_LOOKUP.get();
    }

    @Override
    public PubSubConsumerAdapter create(PubSubConsumerAdapterContext context) {
      OBSERVED_ENCRYPTION_KEY_URN_LOOKUP.set(context.getPubSubEncryptionKeyUrnLookup());

      PubSubConsumerAdapter consumer = mock(PubSubConsumerAdapter.class);
      AtomicReference<PubSubTopicPartition> subscribedPartition = new AtomicReference<>();
      doAnswer(invocation -> {
        subscribedPartition.set(invocation.getArgument(0));
        return null;
      }).when(consumer).subscribe(any(PubSubTopicPartition.class), any(PubSubPosition.class));
      doAnswer(
          invocation -> Collections.singletonMap(
              subscribedPartition.get(),
              Collections.singletonList(createStartOfPushMessage(subscribedPartition.get())))).when(consumer)
                  .poll(anyLong());
      return consumer;
    }

    private static DefaultPubSubMessage createStartOfPushMessage(PubSubTopicPartition topicPartition) {
      StartOfPush startOfPush = new StartOfPush();
      startOfPush.compressionStrategy = CompressionStrategy.ZSTD_WITH_DICT.getValue();
      startOfPush.compressionDictionary = ByteBuffer.wrap(DICTIONARY_TO_SERVE);

      ControlMessage controlMessage = new ControlMessage();
      controlMessage.controlMessageType = ControlMessageType.START_OF_PUSH.getValue();
      controlMessage.controlMessageUnion = startOfPush;
      KafkaMessageEnvelope envelope =
          new KafkaMessageEnvelope(MessageType.CONTROL_MESSAGE.getValue(), null, controlMessage, null);
      return new ImmutablePubSubMessage(
          new KafkaKey(MessageType.CONTROL_MESSAGE, new byte[0]),
          envelope,
          topicPartition,
          ApacheKafkaOffsetPosition.of(0),
          0L,
          0);
    }

    @Override
    public String getName() {
      return RecordingPubSubConsumerAdapterFactory.class.getSimpleName();
    }

    @Override
    public void close() {
    }
  }
}
