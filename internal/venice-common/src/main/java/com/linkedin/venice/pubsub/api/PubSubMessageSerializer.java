package com.linkedin.venice.pubsub.api;

import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.serialization.KafkaKeySerializer;
import com.linkedin.venice.serialization.VeniceKafkaSerializer;
import com.linkedin.venice.serialization.avro.KafkaValueSerializer;


/**
 * A serializer for PubSub messages that converts {@link KafkaKey} and {@link KafkaMessageEnvelope}
 * into byte arrays for transmission over PubSub systems.
 */
public class PubSubMessageSerializer {
  private final VeniceKafkaSerializer<KafkaKey> keySerializer;
  private final VeniceKafkaSerializer<KafkaMessageEnvelope> valueSerializer;

  /**
   * A default instance of {@link PubSubMessageSerializer} using default {@link KafkaKeySerializer}
   * and {@link KafkaValueSerializer}.
   */
  public static final PubSubMessageSerializer DEFAULT_PUBSUB_SERIALIZER =
      new PubSubMessageSerializer(new KafkaKeySerializer(), new KafkaValueSerializer());

  /**
   * Constructs a {@link PubSubMessageSerializer} with the specified key and value serializers.
   *
   * @param keySerializer The serializer for {@link KafkaKey}.
   * @param valueSerializer The serializer for {@link KafkaMessageEnvelope}.
   */
  public PubSubMessageSerializer(
      VeniceKafkaSerializer<KafkaKey> keySerializer,
      VeniceKafkaSerializer<KafkaMessageEnvelope> valueSerializer) {
    this.keySerializer = keySerializer;
    this.valueSerializer = valueSerializer;
  }

  /**
   * Serializes a {@link KafkaKey} into a byte array.
   *
   * @param topicPartition The {@link PubSubTopicPartition} that the key belongs to.
   * @param key The {@link KafkaKey} to be serialized.
   * @return The serialized byte array representation of the key.
   */
  public byte[] serializeKey(PubSubTopicPartition topicPartition, KafkaKey key) {
    return serializeKey(topicPartition.getTopicName(), key);
  }

  public byte[] serializeKey(String topicName, KafkaKey key) {
    return keySerializer.serialize(topicName, key);
  }

  /**
   * Serializes a {@link KafkaMessageEnvelope} into a byte array.
   *
   * @param topicPartition The {@link PubSubTopicPartition} that the value belongs to.
   * @param value The {@link KafkaMessageEnvelope} to be serialized.
   * @return The serialized byte array representation of the value.
   */
  public byte[] serializeValue(PubSubTopicPartition topicPartition, KafkaMessageEnvelope value) {
    return serializeValue(topicPartition.getTopicName(), value);
  }

  public byte[] serializeValue(String topicName, KafkaMessageEnvelope value) {
    return valueSerializer.serialize(topicName, value);
  }
}
