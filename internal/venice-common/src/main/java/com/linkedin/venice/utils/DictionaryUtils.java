package com.linkedin.venice.utils;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.kafka.protocol.ControlMessage;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.StartOfPush;
import com.linkedin.venice.kafka.protocol.enums.ControlMessageType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.pubsub.PubSubClientsFactory;
import com.linkedin.venice.pubsub.PubSubConsumerAdapterContext;
import com.linkedin.venice.pubsub.PubSubConsumerAdapterFactory;
import com.linkedin.venice.pubsub.PubSubPositionTypeRegistry;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.DefaultPubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubMessageDeserializer;
import com.linkedin.venice.pubsub.api.PubSubSymbolicPosition;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class DictionaryUtils {
  private static final Logger LOGGER = LogManager.getLogger(DictionaryUtils.class);

  private static VeniceProperties getKafkaConsumerProps(VeniceProperties veniceProperties) {
    Properties props = veniceProperties.toProperties();
    // Increase receive buffer to 1MB to check whether it can solve the metadata timing out issue
    // TODO: Remove hardcoded buffer size and pass it down from the job config.
    props.put(ConfigKeys.KAFKA_CONFIG_PREFIX + ConsumerConfig.RECEIVE_BUFFER_CONFIG, 1024 * 1024);
    return new VeniceProperties(props);
  }

  public static ByteBuffer readDictionaryFromKafka(String topicName, VeniceProperties props) {
    return readDictionaryFromKafka(topicName, props, PubSubMessageDeserializer.createDefaultDeserializer());
  }

  /**
   * Every overload that accepts a {@code pubSubEncryptionKeyUrnLookup} uses the {@code WithEncryptionLookup} name
   * instead of being overloaded on parameter type alongside the lookup-free {@link #readDictionaryFromKafka}
   * overloads, for two reasons: (1) a {@link Function} and a {@link PubSubMessageDeserializer} are unrelated
   * reference types, so a caller passing a literal {@code null} argument in that position would otherwise make
   * the two overload sets ambiguous and fail to compile; (2) the method name alone then tells a caller/reader
   * whether a given read path resolves V2 (li-crypt) encryption keys — grepping for "WithEncryptionLookup" finds
   * every encryption-aware call site, while every {@link #readDictionaryFromKafka} call is guaranteed to not
   * support V2-encrypted dictionaries.
   *
   * @param pubSubEncryptionKeyUrnLookup Optional store-name to raw encryption-key URN lookup. Required to decode a
   *                                     V2 (li-crypt) encrypted Start Of Push message; pass {@code null} when the
   *                                     source topic is not V2-encrypted.
   */
  public static ByteBuffer readDictionaryFromKafkaWithEncryptionLookup(
      String topicName,
      VeniceProperties props,
      Function<String, String> pubSubEncryptionKeyUrnLookup) {
    return readDictionaryFromKafkaWithEncryptionLookup(
        topicName,
        props,
        PubSubMessageDeserializer.createDefaultDeserializer(),
        pubSubEncryptionKeyUrnLookup);
  }

  public static ByteBuffer readDictionaryFromKafka(
      String topicName,
      VeniceProperties props,
      PubSubMessageDeserializer pubSubMessageDeserializer) {
    return readDictionaryFromKafkaWithEncryptionLookup(topicName, props, pubSubMessageDeserializer, null);
  }

  /**
   * @param pubSubEncryptionKeyUrnLookup Optional store-name to raw encryption-key URN lookup. Required to decode a
   *                                     V2 (li-crypt) encrypted Start Of Push message; pass {@code null} when the
   *                                     source topic is not V2-encrypted.
   */
  public static ByteBuffer readDictionaryFromKafkaWithEncryptionLookup(
      String topicName,
      VeniceProperties props,
      PubSubMessageDeserializer pubSubMessageDeserializer,
      Function<String, String> pubSubEncryptionKeyUrnLookup) {
    PubSubConsumerAdapterFactory pubSubConsumerAdapterFactory = PubSubClientsFactory.createConsumerFactory(props);
    PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();
    VeniceProperties pubSubProperties = getKafkaConsumerProps(props);
    PubSubConsumerAdapterContext context =
        new PubSubConsumerAdapterContext.Builder().setVeniceProperties(pubSubProperties)
            .setPubSubTopicRepository(pubSubTopicRepository)
            .setPubSubMessageDeserializer(pubSubMessageDeserializer)
            .setPubSubPositionTypeRegistry(PubSubPositionTypeRegistry.fromPropertiesOrDefault(pubSubProperties))
            .setPubSubEncryptionKeyUrnLookup(pubSubEncryptionKeyUrnLookup)
            .setConsumerName("DictionaryUtilsConsumer")
            .build();
    try (PubSubConsumerAdapter pubSubConsumer = pubSubConsumerAdapterFactory.create(context)) {
      return DictionaryUtils.readDictionaryFromKafka(topicName, pubSubConsumer, pubSubTopicRepository);
    }
  }

  /**
   * This function reads the kafka topic for the store version for the Start Of Push message which contains the
   * compression dictionary. Once the Start of Push message has been read, the consumer stops.
   * @return The compression dictionary wrapped in a ByteBuffer, or null if no dictionary was present in the
   * Start Of Push message.
   */
  public static ByteBuffer readDictionaryFromKafka(
      String topicName,
      PubSubConsumerAdapter pubSubConsumer,
      PubSubTopicRepository pubSubTopicRepository) {
    LOGGER.info("Consuming from topic: {} till StartOfPush", topicName);
    PubSubTopic pubSubTopic = pubSubTopicRepository.getTopic(topicName);
    PubSubTopicPartition pubSubTopicPartition = new PubSubTopicPartitionImpl(pubSubTopic, 0);
    pubSubConsumer.subscribe(pubSubTopicPartition, PubSubSymbolicPosition.EARLIEST);
    try {
      boolean startOfPushReceived = false;
      ByteBuffer compressionDictionary = null;

      KafkaKey kafkaKey;
      KafkaMessageEnvelope kafkaValue = null;
      while (!startOfPushReceived) {
        Map<PubSubTopicPartition, List<DefaultPubSubMessage>> messages = pubSubConsumer.poll(10 * Time.MS_PER_SECOND);

        if (!messages.containsKey(pubSubTopicPartition)) {
          continue;
        }

        for (final DefaultPubSubMessage message: messages.get(pubSubTopicPartition)) {
          kafkaKey = message.getKey();
          kafkaValue = message.getValue();
          if (kafkaKey.isControlMessage()) {
            ControlMessage controlMessage = (ControlMessage) kafkaValue.payloadUnion;
            ControlMessageType type = ControlMessageType.valueOf(controlMessage);
            LOGGER
                .info("Consumed ControlMessage: {} from topic partition: {}", type.name(), message.getTopicPartition());
            if (type == ControlMessageType.START_OF_PUSH) {
              startOfPushReceived = true;
              compressionDictionary = ((StartOfPush) controlMessage.controlMessageUnion).compressionDictionary;
              if (compressionDictionary == null || !compressionDictionary.hasRemaining()) {
                LOGGER.warn(
                    "No dictionary present in Start of Push message from topic partition: {}",
                    message.getTopicPartition());
                return null;
              }
              break;
            }
          } else {
            LOGGER.error(
                "Consumed non Control Message before Start of Push from topic partition: {}",
                message.getTopicPartition());
            return null;
          }
        }
      }
      return compressionDictionary;
    } finally {
      pubSubConsumer.unSubscribe(pubSubTopicPartition);
    }
  }
}
