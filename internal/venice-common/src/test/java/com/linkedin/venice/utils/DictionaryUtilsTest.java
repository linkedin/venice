package com.linkedin.venice.utils;

import static com.linkedin.venice.ConfigKeys.PUBSUB_BROKER_ADDRESS;
import static com.linkedin.venice.ConfigKeys.PUBSUB_CONSUMER_ADAPTER_FACTORY_CLASS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

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

  @DataProvider
  public Object[][] dictionaryReaders() {
    return new Object[][] { { "prebuilt", 0 }, { "legacy", 1 }, { "custom", 1 }, { "encrypted", 1 },
        { "encrypted-custom", 1 }, { "missing-lookup", 0 }, { "missing-lookup-custom", 0 } };
  }

  @Test(dataProvider = "dictionaryReaders")
  public void testGetDictionary(String reader, int expectedCloses) {
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

    RecordingPubSubConsumerAdapterFactory.reset(pubSubConsumer);
    Properties props = new Properties();
    props.setProperty(PUBSUB_BROKER_ADDRESS, "localhost:9092");
    props.setProperty(PUBSUB_CONSUMER_ADAPTER_FACTORY_CLASS, RecordingPubSubConsumerAdapterFactory.class.getName());
    VeniceProperties consumerProperties = new VeniceProperties(props);
    PubSubMessageDeserializer deserializer = mock(PubSubMessageDeserializer.class);
    Function<String, String> lookup = store -> "urn:li:test-key";
    if (reader.startsWith("missing-lookup")) {
      if (reader.equals("missing-lookup")) {
        Assert.expectThrows(
            VeniceException.class,
            () -> DictionaryUtils.readDictionaryFromEncryptedKafka(topic.getName(), consumerProperties, null));
      } else {
        Assert.expectThrows(
            VeniceException.class,
            () -> DictionaryUtils
                .readDictionaryFromEncryptedKafka(topic.getName(), consumerProperties, deserializer, null));
      }
      Assert.assertNull(RecordingPubSubConsumerAdapterFactory.OBSERVED_CONTEXT.get());
      verifyNoInteractions(pubSubConsumer);
      return;
    }
    ByteBuffer dictionaryFromKafka;
    switch (reader) {
      case "prebuilt":
        dictionaryFromKafka =
            DictionaryUtils.readDictionaryFromKafka(topic.getName(), pubSubConsumer, pubSubTopicRepository);
        break;
      case "legacy":
        dictionaryFromKafka = DictionaryUtils.readDictionaryFromKafka(topic.getName(), consumerProperties);
        break;
      case "custom":
        dictionaryFromKafka =
            DictionaryUtils.readDictionaryFromKafka(topic.getName(), consumerProperties, deserializer);
        break;
      case "encrypted":
        dictionaryFromKafka =
            DictionaryUtils.readDictionaryFromEncryptedKafka(topic.getName(), consumerProperties, lookup);
        break;
      case "encrypted-custom":
        dictionaryFromKafka =
            DictionaryUtils.readDictionaryFromEncryptedKafka(topic.getName(), consumerProperties, deserializer, lookup);
        break;
      default:
        throw new AssertionError("Unknown reader: " + reader);
    }
    Assert.assertEquals(dictionaryFromKafka.array(), dictionaryToSend);
    verify(pubSubConsumer, times(1)).subscribe(eq(topicPartition), any(PubSubPosition.class));
    verify(pubSubConsumer, times(1)).unSubscribe(topicPartition);
    verify(pubSubConsumer, times(1)).poll(anyLong());
    verify(pubSubConsumer, times(expectedCloses)).close();
    if (expectedCloses > 0) {
      PubSubConsumerAdapterContext context = RecordingPubSubConsumerAdapterFactory.OBSERVED_CONTEXT.get();
      Assert.assertSame(context.getPubSubEncryptionKeyUrnLookup(), reader.startsWith("encrypted") ? lookup : null);
      if (reader.endsWith("custom")) {
        Assert.assertSame(context.getPubSubMessageDeserializer(), deserializer);
      } else {
        Assert.assertNotNull(context.getPubSubMessageDeserializer());
      }
    }
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

  public static class RecordingPubSubConsumerAdapterFactory
      extends PubSubConsumerAdapterFactory<PubSubConsumerAdapter> {
    private static final AtomicReference<PubSubConsumerAdapterContext> OBSERVED_CONTEXT = new AtomicReference<>();
    private static final AtomicReference<PubSubConsumerAdapter> OBSERVED_CONSUMER = new AtomicReference<>();

    static void reset(PubSubConsumerAdapter consumer) {
      OBSERVED_CONTEXT.set(null);
      OBSERVED_CONSUMER.set(consumer);
    }

    @Override
    public PubSubConsumerAdapter create(PubSubConsumerAdapterContext context) {
      OBSERVED_CONTEXT.set(context);
      return OBSERVED_CONSUMER.get();
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
