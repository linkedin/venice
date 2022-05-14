package com.linkedin.venice.writer;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.partitioner.DefaultVenicePartitioner;
import com.linkedin.venice.partitioner.VenicePartitioner;
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
  private final String localKafkaBootstrapServers;
  private final Optional<SharedKafkaProducerService> sharedKafkaProducerService;

  public VeniceWriterFactory(Properties properties) {
    this(properties, Optional.empty());
  }

  public VeniceWriterFactory(Properties properties, Optional<SharedKafkaProducerService> sharedKafkaProducerService) {
    this.properties = properties;
    this.sharedKafkaProducerService = sharedKafkaProducerService;
    boolean sslToKafka = Boolean.valueOf(properties.getProperty(ConfigKeys.SSL_TO_KAFKA, "false"));
    if (!sslToKafka) {
      checkProperty(ConfigKeys.KAFKA_BOOTSTRAP_SERVERS);
      localKafkaBootstrapServers = properties.getProperty(ConfigKeys.KAFKA_BOOTSTRAP_SERVERS);
    } else {
      checkProperty(ConfigKeys.SSL_KAFKA_BOOTSTRAP_SERVERS);
      localKafkaBootstrapServers = properties.getProperty(ConfigKeys.SSL_KAFKA_BOOTSTRAP_SERVERS);
    }
  }

  /**
   * TODO: Consider adding a VeniceWriterConfig class, so that we have less function overloads in this class.
   */

  /**
   * Create a basic venice writer with default serializer.
   */
  public VeniceWriter<byte[], byte[], byte[]> createBasicVeniceWriter(String topicName, Time time) {
    return createVeniceWriter(topicName, new DefaultSerializer(), new DefaultSerializer(), new DefaultSerializer(),
        Optional.empty(), time);
  }

  public VeniceWriter<byte[], byte[], byte[]> createBasicVeniceWriter(String topicName, Time time, VenicePartitioner partitioner, int topicPartitionCount) {
    return createVeniceWriter(topicName, this.localKafkaBootstrapServers, new DefaultSerializer(), new DefaultSerializer(),
        new DefaultSerializer(), Optional.empty(), time, partitioner, Optional.of(topicPartitionCount), Optional.empty());
  }

  public VeniceWriter<byte[], byte[], byte[]> createBasicVeniceWriter(String topicName, Time time,
      boolean chunkingEnabled, VenicePartitioner partitioner, Optional<Integer> topicPartitionCount) {
    return createVeniceWriter(topicName, this.localKafkaBootstrapServers, new DefaultSerializer(), new DefaultSerializer(),
        new DefaultSerializer(), Optional.of(chunkingEnabled), time, partitioner, topicPartitionCount, Optional.empty());
  }

  /** test-only */
  public VeniceWriter<byte[], byte[], byte[]> createBasicVeniceWriter(String topicName) {
    return createBasicVeniceWriter(topicName, SystemTime.INSTANCE);
  }

  public VeniceWriter<byte[], byte[], byte[]> createBasicVeniceWriter(String topicName, Optional<Boolean> chunkingEnabled,
      VenicePartitioner partitioner, int topicPartitionCount) {
    return createVeniceWriter(topicName, new DefaultSerializer(), new DefaultSerializer(), new DefaultSerializer(),
        chunkingEnabled, SystemTime.INSTANCE, partitioner, Optional.of(topicPartitionCount), Optional.empty());
  }

  public <K, V> VeniceWriter<K, V, byte[]> createVeniceWriter(String topicName, VeniceKafkaSerializer<K> keySerializer,
      VeniceKafkaSerializer<V> valueSerializer) {
    return createVeniceWriter(topicName, keySerializer, valueSerializer, new DefaultSerializer(),
        Optional.empty(), SystemTime.INSTANCE);
  }

  public <K, V> VeniceWriter<K, V, byte[]> createVeniceWriter(String topicName, VeniceKafkaSerializer<K> keySerializer,
      VeniceKafkaSerializer<V> valueSerializer, int partitionCount, VenicePartitioner partitioner) {
    return createVeniceWriter(topicName, this.localKafkaBootstrapServers, keySerializer, valueSerializer,
        new DefaultSerializer(), Optional.empty(), SystemTime.INSTANCE, partitioner, Optional.of(partitionCount), Optional.empty());
  }

  /**
   * test-only
   *
   * @param chunkingEnabled override the factory's default chunking setting.
   */
  public <K, V> VeniceWriter<K, V, byte[]> createVeniceWriter(String topicName, VeniceKafkaSerializer<K> keySerializer,
      VeniceKafkaSerializer<V> valueSerializer, boolean chunkingEnabled) {
    return createVeniceWriter(topicName, keySerializer, valueSerializer, new DefaultSerializer(),
        Optional.of(chunkingEnabled), SystemTime.INSTANCE);
  }

  public <K, V, U> VeniceWriter<K, V, U> createVeniceWriter(String topic, VeniceKafkaSerializer<K> keySerializer,
      VeniceKafkaSerializer<V> valueSerializer, VeniceKafkaSerializer<U> writeComputeSerializer, Optional<Boolean> chunkingEnabled, Time time) {
    return createVeniceWriter(topic, this.localKafkaBootstrapServers, keySerializer, valueSerializer, writeComputeSerializer, chunkingEnabled, time, new DefaultVenicePartitioner(), Optional.empty(), Optional.empty());
  }

  public <K, V, U> VeniceWriter<K, V, U> createVeniceWriter(String topic, VeniceKafkaSerializer<K> keySerializer,
      VeniceKafkaSerializer<V> valueSerializer, VeniceKafkaSerializer<U> writeComputeSerializer, Optional<Boolean> chunkingEnabled, Time time, VenicePartitioner partitioner,
      Optional<Integer> topicPartitionCount, Optional<Integer> targetStoreVersionForIncPush) {
    return createVeniceWriter(topic, this.localKafkaBootstrapServers, keySerializer, valueSerializer, writeComputeSerializer, chunkingEnabled, time, partitioner, topicPartitionCount, targetStoreVersionForIncPush);
  }

  /**
   * Create a {@link VeniceWriter} which is used to communicate with the real-time topic.
   *
   * @param chunkingEnabled override the factory's default chunking setting.
   */
  protected <K, V, U> VeniceWriter<K, V, U> createVeniceWriter(String topic, String kafkaBootstrapServers, VeniceKafkaSerializer<K> keySerializer,
      VeniceKafkaSerializer<V> valueSerializer, VeniceKafkaSerializer<U> writeComputeSerializer, Optional<Boolean> chunkingEnabled, Time time, VenicePartitioner partitioner,
      Optional<Integer> topicPartitionCount, Optional<Integer> targetStoreVersionForIncPush) {
    //Currently this writerProperties is overloaded as it contains KafkaProducer config and as well as VeniceWriter config
    //We should clean this up and also not add any more KafkaProducer config here.
    Properties writerProperties = new Properties();
    writerProperties.putAll(this.properties);
    writerProperties.put(ConfigKeys.KAFKA_BOOTSTRAP_SERVERS, kafkaBootstrapServers);
    if (chunkingEnabled.isPresent()) {
      writerProperties.put(VeniceWriter.ENABLE_CHUNKING, chunkingEnabled.get());
    }
    VeniceProperties props = new VeniceProperties(writerProperties);
    return new VeniceWriter<>(props, topic, keySerializer, valueSerializer, writeComputeSerializer, partitioner, time,
        topicPartitionCount, targetStoreVersionForIncPush, () -> {
      if (sharedKafkaProducerService.isPresent()) {
        return sharedKafkaProducerService.get().acquireKafkaProducer(topic);
      } else {
        return new ApacheKafkaProducer(props);
      }
    });
  }

  public VeniceWriter<KafkaKey, byte[], byte[]> createVeniceWriter(String topic, int partitionCount) {
    return createVeniceWriter(topic, new KafkaKeySerializer(), new DefaultSerializer(), new DefaultSerializer(),
        Optional.empty(), SystemTime.INSTANCE, new DefaultVenicePartitioner(), Optional.of(partitionCount), Optional.empty());
  }

  public VeniceWriter<KafkaKey, byte[], byte[]> createVeniceWriter(String topic, VenicePartitioner partitioner,
      Optional<Integer> targetStoreVersionForIncPush, int partitionCount) {
    return createVeniceWriter(topic, new KafkaKeySerializer(), new DefaultSerializer(), new DefaultSerializer(),
        Optional.empty(), SystemTime.INSTANCE, partitioner, Optional.of(partitionCount), targetStoreVersionForIncPush);
  }

  public VeniceWriter<KafkaKey, byte[], byte[]> createVeniceWriter(String topic, String kafkaBootstrapServers, int partitionCount) {
    return createVeniceWriter(topic, kafkaBootstrapServers, new KafkaKeySerializer(), new DefaultSerializer(), new DefaultSerializer(),
        Optional.empty(), SystemTime.INSTANCE, new DefaultVenicePartitioner(), Optional.of(partitionCount), Optional.empty());
  }

  private void checkProperty(String key) {
    if (!properties.containsKey(key)) {
      throw new VeniceException(
          "Invalid properties for Venice writer factory. Required property: " + key + " is missing.");
    }
  }
}