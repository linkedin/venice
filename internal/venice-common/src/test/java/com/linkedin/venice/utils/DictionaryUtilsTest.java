package com.linkedin.venice.utils;

import static com.linkedin.venice.ConfigKeys.PUBSUB_BROKER_ADDRESS;
import static com.linkedin.venice.ConfigKeys.PUBSUB_CONSUMER_ADAPTER_FACTORY_CLASS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.exceptions.VeniceException;
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
import com.linkedin.venice.pubsub.api.PubSubMessageDeserializer;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
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
    verify(pubSubConsumer, never()).close();
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

  @DataProvider
  public Object[][] dictionaryReaderPaths() {
    return new Object[][] { { false, false }, { false, true }, { true, false } };
  }

  @Test(dataProvider = "dictionaryReaderPaths")
  public void testDictionaryReaderPathsPreserveContextAndLifecycle(boolean encrypted, boolean customDeserializer) {
    RecordingPubSubConsumerAdapterFactory.reset();
    Function<String, String> encryptionKeyUrnLookup = storeName -> "urn:li:dataEncryptionKey:test-key";

    Properties props = new Properties();
    props.setProperty(PUBSUB_BROKER_ADDRESS, "localhost:9092");
    props.setProperty(PUBSUB_CONSUMER_ADAPTER_FACTORY_CLASS, RecordingPubSubConsumerAdapterFactory.class.getName());

    PubSubMessageDeserializer deserializer = mock(PubSubMessageDeserializer.class);
    ByteBuffer dictionaryFromKafka;
    if (encrypted) {
      dictionaryFromKafka = DictionaryUtils
          .readDictionaryFromEncryptedKafka(getTopic().getName(), new VeniceProperties(props), encryptionKeyUrnLookup);
    } else if (customDeserializer) {
      dictionaryFromKafka =
          DictionaryUtils.readDictionaryFromKafka(getTopic().getName(), new VeniceProperties(props), deserializer);
    } else {
      dictionaryFromKafka = DictionaryUtils.readDictionaryFromKafka(getTopic().getName(), new VeniceProperties(props));
    }

    Assert.assertEquals(dictionaryFromKafka.array(), RecordingPubSubConsumerAdapterFactory.DICTIONARY_TO_SERVE);
    Function<String, String> observedLookup = RecordingPubSubConsumerAdapterFactory.getObservedEncryptionKeyUrnLookup();
    if (encrypted) {
      Assert.assertSame(observedLookup, encryptionKeyUrnLookup);
    } else {
      Assert.assertNull(observedLookup);
    }
    if (customDeserializer) {
      Assert.assertSame(
          RecordingPubSubConsumerAdapterFactory.OBSERVED_CONTEXT.get().getPubSubMessageDeserializer(),
          deserializer);
    }
    verify(RecordingPubSubConsumerAdapterFactory.OBSERVED_CONSUMER.get()).close();
    verify(RecordingPubSubConsumerAdapterFactory.OBSERVED_CONSUMER.get()).poll(anyLong());
  }

  @Test
  public void testEncryptedDictionaryRejectsMissingLookupBeforeConsumerCreation() {
    RecordingPubSubConsumerAdapterFactory.reset();

    Properties props = new Properties();
    props.setProperty(PUBSUB_BROKER_ADDRESS, "localhost:9092");
    props.setProperty(PUBSUB_CONSUMER_ADAPTER_FACTORY_CLASS, RecordingPubSubConsumerAdapterFactory.class.getName());

    Assert.expectThrows(
        VeniceException.class,
        () -> DictionaryUtils
            .readDictionaryFromEncryptedKafka(getTopic().getName(), new VeniceProperties(props), null));
    Assert.assertNull(RecordingPubSubConsumerAdapterFactory.OBSERVED_CONTEXT.get());
  }

  public static class RecordingPubSubConsumerAdapterFactory
      extends PubSubConsumerAdapterFactory<PubSubConsumerAdapter> {
    static final byte[] DICTIONARY_TO_SERVE = "TEST_DICT".getBytes();
    private static final AtomicReference<PubSubConsumerAdapterContext> OBSERVED_CONTEXT = new AtomicReference<>();
    private static final AtomicReference<PubSubConsumerAdapter> OBSERVED_CONSUMER = new AtomicReference<>();
    private static final AtomicReference<Function<String, String>> OBSERVED_ENCRYPTION_KEY_URN_LOOKUP =
        new AtomicReference<>();

    static void reset() {
      OBSERVED_CONTEXT.set(null);
      OBSERVED_CONSUMER.set(null);
      OBSERVED_ENCRYPTION_KEY_URN_LOOKUP.set(null);
    }

    static Function<String, String> getObservedEncryptionKeyUrnLookup() {
      return OBSERVED_ENCRYPTION_KEY_URN_LOOKUP.get();
    }

    @Override
    public PubSubConsumerAdapter create(PubSubConsumerAdapterContext context) {
      OBSERVED_CONTEXT.set(context);
      OBSERVED_ENCRYPTION_KEY_URN_LOOKUP.set(context.getPubSubEncryptionKeyUrnLookup());

      PubSubConsumerAdapter consumer = mock(PubSubConsumerAdapter.class);
      OBSERVED_CONSUMER.set(consumer);
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
