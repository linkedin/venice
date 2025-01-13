package com.linkedin.venice.utils;

import com.linkedin.venice.kafka.protocol.ControlMessage;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.StartOfPush;
import com.linkedin.venice.kafka.protocol.enums.ControlMessageType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.pubsub.PubSubConsumerAdapterFactory;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.adapter.kafka.consumer.ApacheKafkaConsumerAdapterFactory;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubMessageDeserializer;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.serialization.avro.KafkaValueSerializer;
import com.linkedin.venice.utils.pools.LandFillObjectPool;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class DictionaryUtils {
  private static final Logger LOGGER = LogManager.getLogger(DictionaryUtils.class);

  private static VeniceProperties getKafkaConsumerProps(VeniceProperties veniceProperties) {
    Properties props = veniceProperties.toProperties();
    // Increase receive buffer to 1MB to check whether it can solve the metadata timing out issue
    props.put(ConsumerConfig.RECEIVE_BUFFER_CONFIG, 1024 * 1024);
    return new VeniceProperties(props);
  }

  public static ByteBuffer readDictionaryFromKafka(String topicName, VeniceProperties props) {
    PubSubConsumerAdapterFactory pubSubConsumerAdapterFactory = new ApacheKafkaConsumerAdapterFactory();
    PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();
    PubSubMessageDeserializer pubSubMessageDeserializer = new PubSubMessageDeserializer(
        new KafkaValueSerializer(),
        new LandFillObjectPool<>(KafkaMessageEnvelope::new),
        new LandFillObjectPool<>(KafkaMessageEnvelope::new));
    try (PubSubConsumerAdapter pubSubConsumer = pubSubConsumerAdapterFactory
        .create(getKafkaConsumerProps(props), false, pubSubMessageDeserializer, "DictionaryUtilsConsumer")) {
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
    pubSubConsumer.subscribe(pubSubTopicPartition, 0);
    try {
      boolean startOfPushReceived = false;
      ByteBuffer compressionDictionary = null;

      KafkaKey kafkaKey;
      KafkaMessageEnvelope kafkaValue = null;
      while (!startOfPushReceived) {
        Map<PubSubTopicPartition, List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>>> messages =
            pubSubConsumer.poll(10 * Time.MS_PER_SECOND);

        if (!messages.containsKey(pubSubTopicPartition)) {
          continue;
        }

        for (final PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> message: messages.get(pubSubTopicPartition)) {
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
