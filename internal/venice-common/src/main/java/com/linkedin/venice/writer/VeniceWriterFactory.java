package com.linkedin.venice.writer;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.partitioner.VenicePartitioner;
import com.linkedin.venice.serialization.VeniceKafkaSerializer;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.Optional;
import java.util.Properties;


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

  private void checkProperty(String key) {
    if (!properties.containsKey(key)) {
      throw new VeniceException(
          "Invalid properties for Venice writer factory. Required property: " + key + " is missing.");
    }
  }

  public <K, V, U> VeniceWriter<K, V, U> createVeniceWriter(VeniceWriterOptions options) {
    // Currently this writerProperties is overloaded as it contains KafkaProducer config and as well as
    // VeniceWriter config. We should clean this up and also not add more KafkaProducer config here.
    Properties writerProperties = new Properties();
    writerProperties.putAll(this.properties);

    if (options.getKafkaBootstrapServers() != null) {
      writerProperties.put(ConfigKeys.KAFKA_BOOTSTRAP_SERVERS, options.getKafkaBootstrapServers());
    } else {
      writerProperties.put(ConfigKeys.KAFKA_BOOTSTRAP_SERVERS, localKafkaBootstrapServers);
    }
    writerProperties.put(VeniceWriter.ENABLE_CHUNKING, options.isChunkingEnabled());
    VeniceProperties props = new VeniceProperties(writerProperties);
    return new VeniceWriter<>(options, props, () -> {
      if (sharedKafkaProducerService.isPresent()) {
        return sharedKafkaProducerService.get().acquireKafkaProducer(options.getTopicName());
      } else {
        return new ApacheKafkaProducer(props);
      }
    });
  }

  /*
   * Marking these classes as deprecated as they are used in other projects and may break them.
   * Please DO NOT use the following deprecated methods. Instead, we should construct VeniceWriterOptions
   * object and pass it to createVeniceWriter(VeniceWriterOptions options).
   *
   * If you happen to change the code in a file where Deprecated createBasicVeniceWriter/createVeniceWriter is used
   * please replace it with the code found in the deprecated method: createVeniceWriter(VeniceWriterOptions)}.
   *
   * The following deprecated methods will be deleted once all clients are upgraded the release containing
   * the new code.
   */

  /**
   * Create a basic venice writer with default serializer.
   *
   * @deprecated
   * This method is being deprecated and will be removed in the next release.
   * <p> Use {@link VeniceWriterFactory#createVeniceWriter(VeniceWriterOptions)} instead.
   *
   */
  @Deprecated
  public VeniceWriter<byte[], byte[], byte[]> createBasicVeniceWriter(String topicName, Time time) {
    VeniceWriterOptions options = new VeniceWriterOptions.Builder(topicName).setTime(time).build();
    return createVeniceWriter(options);
  }

  @Deprecated
  public VeniceWriter<byte[], byte[], byte[]> createBasicVeniceWriter(
      String topicName,
      Time time,
      VenicePartitioner partitioner,
      int topicPartitionCount) {
    VeniceWriterOptions options = new VeniceWriterOptions.Builder(topicName).setTime(time)
        .setPartitioner(partitioner)
        .setPartitionCount(Optional.of(topicPartitionCount))
        .build();
    return createVeniceWriter(options);
  }

  @Deprecated
  public VeniceWriter<byte[], byte[], byte[]> createBasicVeniceWriter(
      String topicName,
      Time time,
      boolean chunkingEnabled,
      VenicePartitioner partitioner,
      Optional<Integer> topicPartitionCount) {
    VeniceWriterOptions options = new VeniceWriterOptions.Builder(topicName).setTime(time)
        .setPartitioner(partitioner)
        .setPartitionCount(topicPartitionCount)
        .setChunkingEnabled(chunkingEnabled)
        .build();
    return createVeniceWriter(options);
  }

  /** test-only */
  @Deprecated
  public VeniceWriter<byte[], byte[], byte[]> createBasicVeniceWriter(String topicName) {
    return createVeniceWriter(new VeniceWriterOptions.Builder(topicName).build());
  }

  @Deprecated
  public VeniceWriter<byte[], byte[], byte[]> createBasicVeniceWriter(
      String topicName,
      boolean chunkingEnabled,
      VenicePartitioner partitioner,
      int topicPartitionCount) {
    VeniceWriterOptions options = new VeniceWriterOptions.Builder(topicName).setPartitioner(partitioner)
        .setPartitionCount(Optional.of(topicPartitionCount))
        .setChunkingEnabled(chunkingEnabled)
        .build();
    return createVeniceWriter(options);
  }

  @Deprecated
  public <K, V> VeniceWriter<K, V, byte[]> createVeniceWriter(
      String topicName,
      VeniceKafkaSerializer<K> keySerializer,
      VeniceKafkaSerializer<V> valueSerializer) {
    VeniceWriterOptions options = new VeniceWriterOptions.Builder(topicName).setKeySerializer(keySerializer)
        .setValueSerializer(valueSerializer)
        .build();
    return createVeniceWriter(options);
  }

  @Deprecated
  public <K, V> VeniceWriter<K, V, byte[]> createVeniceWriter(
      String topicName,
      VeniceKafkaSerializer<K> keySerializer,
      VeniceKafkaSerializer<V> valueSerializer,
      int partitionCount,
      VenicePartitioner partitioner) {
    VeniceWriterOptions options = new VeniceWriterOptions.Builder(topicName).setKeySerializer(keySerializer)
        .setValueSerializer(valueSerializer)
        .setPartitionCount(Optional.of(partitionCount))
        .setPartitioner(partitioner)
        .build();
    return createVeniceWriter(options);
  }

  /**
   * test-only
   *
   * @param chunkingEnabled override the factory's default chunking setting.
   */
  @Deprecated
  public <K, V> VeniceWriter<K, V, byte[]> createVeniceWriter(
      String topicName,
      VeniceKafkaSerializer<K> keySerializer,
      VeniceKafkaSerializer<V> valueSerializer,
      boolean chunkingEnabled) {
    VeniceWriterOptions options = new VeniceWriterOptions.Builder(topicName).setKeySerializer(keySerializer)
        .setValueSerializer(valueSerializer)
        .setChunkingEnabled(chunkingEnabled)
        .build();
    return createVeniceWriter(options);
  }

  @Deprecated
  public <K, V, U> VeniceWriter<K, V, U> createVeniceWriter(
      String topic,
      VeniceKafkaSerializer<K> keySerializer,
      VeniceKafkaSerializer<V> valueSerializer,
      VeniceKafkaSerializer<U> writeComputeSerializer,
      Optional<Boolean> chunkingEnabled,
      Time time) {
    VeniceWriterOptions options = new VeniceWriterOptions.Builder(topic).setKeySerializer(keySerializer)
        .setValueSerializer(valueSerializer)
        .setWriteComputeSerializer(writeComputeSerializer)
        .setChunkingEnabled(chunkingEnabled.orElse(false))
        .setTime(time)
        .build();
    return createVeniceWriter(options);
  }

  // TODO(sumane): Remove this API in the next major release
  /**
   * @deprecated
   * This method is being deprecated and will be removed in the next release.
   * <p> Use {@link VeniceWriterFactory#createVeniceWriter(VeniceWriterOptions)} instead.
   */
  public <K, V, U> VeniceWriter<K, V, U> createVeniceWriter(
      String topic,
      VeniceKafkaSerializer<K> keySerializer,
      VeniceKafkaSerializer<V> valueSerializer,
      VeniceKafkaSerializer<U> writeComputeSerializer,
      Optional<Boolean> chunkingEnabled,
      Time time,
      VenicePartitioner partitioner,
      Optional<Integer> topicPartitionCount,
      Optional<Integer> targetStoreVersionForIncPush) {
    VeniceWriterOptions options = new VeniceWriterOptions.Builder(topic).setKeySerializer(keySerializer)
        .setValueSerializer(valueSerializer)
        .setWriteComputeSerializer(writeComputeSerializer)
        .setChunkingEnabled(chunkingEnabled.orElse(false))
        .setTime(time)
        .setPartitioner(partitioner)
        .setPartitionCount(topicPartitionCount)
        .build();
    return createVeniceWriter(options);
  }

  /**
   * Create a {@link VeniceWriter} which is used to communicate with the real-time topic.
   *
   * @param chunkingEnabled override the factory's default chunking setting.
   */
  @Deprecated
  protected <K, V, U> VeniceWriter<K, V, U> createVeniceWriter(
      String topic,
      String kafkaBootstrapServers,
      VeniceKafkaSerializer<K> keySerializer,
      VeniceKafkaSerializer<V> valueSerializer,
      VeniceKafkaSerializer<U> writeComputeSerializer,
      Optional<Boolean> chunkingEnabled,
      Time time,
      VenicePartitioner partitioner,
      Optional<Integer> topicPartitionCount,
      Optional<Integer> targetStoreVersionForIncPush) {
    VeniceWriterOptions options = new VeniceWriterOptions.Builder(topic).setKafkaBootstrapServers(kafkaBootstrapServers)
        .setKeySerializer(keySerializer)
        .setValueSerializer(valueSerializer)
        .setWriteComputeSerializer(writeComputeSerializer)
        .setChunkingEnabled(chunkingEnabled.orElse(false))
        .setTime(time)
        .setPartitioner(partitioner)
        .setPartitionCount(topicPartitionCount)
        .build();
    return createVeniceWriter(options);
  }

  @Deprecated
  public VeniceWriter<KafkaKey, byte[], byte[]> createVeniceWriter(String topic, int partitionCount) {
    VeniceWriterOptions options = new VeniceWriterOptions.Builder(topic).setUseKafkaKeySerializer(true)
        .setPartitionCount(Optional.of(partitionCount))
        .build();
    return createVeniceWriter(options);
  }

  @Deprecated
  public VeniceWriter<KafkaKey, byte[], byte[]> createVeniceWriter(
      String topic,
      VenicePartitioner partitioner,
      Optional<Integer> targetStoreVersionForIncPush,
      int partitionCount) {
    VeniceWriterOptions options = new VeniceWriterOptions.Builder(topic).setUseKafkaKeySerializer(true)
        .setPartitionCount(Optional.of(partitionCount))
        .setPartitioner(partitioner)
        .build();
    return createVeniceWriter(options);
  }

  @Deprecated
  public VeniceWriter<KafkaKey, byte[], byte[]> createVeniceWriter(
      String topic,
      String kafkaBootstrapServers,
      int partitionCount) {
    VeniceWriterOptions options = new VeniceWriterOptions.Builder(topic).setKafkaBootstrapServers(kafkaBootstrapServers)
        .setUseKafkaKeySerializer(true)
        .setPartitionCount(Optional.of(partitionCount))
        .build();
    return createVeniceWriter(options);
  }
}
