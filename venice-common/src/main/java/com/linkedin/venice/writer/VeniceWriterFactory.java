package com.linkedin.venice.writer;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.serialization.DefaultSerializer;
import com.linkedin.venice.serialization.KafkaKeySerializer;
import com.linkedin.venice.serialization.VeniceKafkaSerializer;
import com.linkedin.venice.utils.SystemTime;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.VeniceProperties;

import java.util.Optional;
import java.util.Properties;
import java.util.function.Consumer;
import java.util.function.Supplier;


/**
 * Factory used to create the venice writer.
 */
public class VeniceWriterFactory {
  private final Properties properties;
  private final String kafkaBootstrapServers;

  public VeniceWriterFactory(Properties properties) {
    this.properties = properties;
    boolean sslToKafka = Boolean.valueOf(properties.getProperty(ConfigKeys.SSL_TO_KAFKA, "false"));
    if (!sslToKafka) {
      checkProperty(ConfigKeys.KAFKA_BOOTSTRAP_SERVERS);
      kafkaBootstrapServers = properties.getProperty(ConfigKeys.KAFKA_BOOTSTRAP_SERVERS);
    } else {
      checkProperty(ConfigKeys.SSL_KAFKA_BOOTSTRAP_SERVERS);
      kafkaBootstrapServers = properties.getProperty(ConfigKeys.SSL_KAFKA_BOOTSTRAP_SERVERS);
    }
  }

  /**
   * Create a basic venice writer with default serializer.
   */
  public VeniceWriter<byte[], byte[], byte[]> createBasicVeniceWriter(String topicName, Time time) {
    return createVeniceWriter(topicName, new DefaultSerializer(), new DefaultSerializer(), new DefaultSerializer(),
        Optional.empty(), time);
  }

  public VeniceWriter<byte[], byte[], byte[]> createBasicVeniceWriter(String topicName) {
    return createBasicVeniceWriter(topicName, SystemTime.INSTANCE);
  }

  /**
   * @param chunkingEnabled override the factory's default chunking setting.
   */
  public VeniceWriter<byte[], byte[], byte[]> createBasicVeniceWriter(String topicName, boolean chunkingEnabled) {
    return createVeniceWriter(topicName, new DefaultSerializer(), new DefaultSerializer(), new DefaultSerializer(),
        Optional.of(chunkingEnabled), SystemTime.INSTANCE);
  }

  public <K, V> VeniceWriter<K, V, byte[]> createVeniceWriter(String topicName, VeniceKafkaSerializer<K> keySerializer,
      VeniceKafkaSerializer<V> valueSerializer) {
    return createVeniceWriter(topicName, keySerializer, valueSerializer, new DefaultSerializer(),
        Optional.empty(), SystemTime.INSTANCE);
  }

  /**
   * @param chunkingEnabled override the factory's default chunking setting.
   */
  public <K, V> VeniceWriter<K, V, byte[]> createVeniceWriter(String topicName, VeniceKafkaSerializer<K> keySerializer,
      VeniceKafkaSerializer<V> valueSerializer, boolean chunkingEnabled) {
    return createVeniceWriter(topicName, keySerializer, valueSerializer, new DefaultSerializer(),
        Optional.of(chunkingEnabled), SystemTime.INSTANCE);
  }

  /**
   * Create a {@link VeniceWriter} which is used to communicate with the real-time topic.
   *
   * @param chunkingEnabled override the factory's default chunking setting.
   */
  protected <K, V, U> VeniceWriter<K, V, U> createVeniceWriter(String topic, VeniceKafkaSerializer<K> keySerializer,
      VeniceKafkaSerializer<V> valueSerializer, VeniceKafkaSerializer<U> writeComputeSerializer, Optional<Boolean> chunkingEnabled, Time time) {
    Properties writerProperties = new Properties();
    writerProperties.putAll(this.properties);
    writerProperties.put(ConfigKeys.KAFKA_BOOTSTRAP_SERVERS, kafkaBootstrapServers);
    if (chunkingEnabled.isPresent()) {
      writerProperties.put(VeniceWriter.ENABLE_CHUNKING, chunkingEnabled.get());
    }
    return new VeniceWriter<>(new VeniceProperties(writerProperties), topic, keySerializer, valueSerializer,
        writeComputeSerializer, time);
  }

  public VeniceWriter<KafkaKey, byte[], byte[]> createVeniceWriter(String topic) {
    return createVeniceWriter(topic, new KafkaKeySerializer(), new DefaultSerializer(), new DefaultSerializer(),
        Optional.empty(), SystemTime.INSTANCE);
  }

  public static void useVeniceWriter(Supplier<VeniceWriter> veniceWriterSupplier, Consumer<VeniceWriter> writerAction) {
    try (VeniceWriter veniceWriter = veniceWriterSupplier.get()) {
      writerAction.accept(veniceWriter);
    }
  }

  private void checkProperty(String key) {
    if (!properties.containsKey(key)) {
      throw new VeniceException(
          "Invalid properties for Venice writer factory. Required property: " + key + " is missing.");
    }
  }
}