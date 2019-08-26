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

import java.util.Properties;


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
  public VeniceWriter<byte[], byte[]> getBasicVeniceWriter(String topicName, Time time) {
    return getVeniceWriter(topicName, new DefaultSerializer(), new DefaultSerializer(), false, time);
  }

  public VeniceWriter<byte[], byte[]> getBasicVeniceWriter(String topicName) {
    return getBasicVeniceWriter(topicName, SystemTime.INSTANCE);
  }

  public VeniceWriter<byte[], byte[]> getBasicVeniceWriter(String topicName, boolean chunkingEnabled) {
    return getVeniceWriter(topicName, new DefaultSerializer(), new DefaultSerializer(), chunkingEnabled, SystemTime.INSTANCE);
  }

  public <K, V> VeniceWriter<K, V> getVeniceWriter(String topicName, VeniceKafkaSerializer<K> keySerializer,
      VeniceKafkaSerializer<V> valueSerializer) {
    return getVeniceWriter(topicName, keySerializer, valueSerializer, false, SystemTime.INSTANCE);
  }

  /**
   * Create a venice writer which is used to communicated with the topic contains real data.
   */
  protected <K, V> VeniceWriter<K, V> getVeniceWriter(String topic, VeniceKafkaSerializer<K> keySerializer,
      VeniceKafkaSerializer<V> valueSerializer, boolean chunkingEnabled, Time time) {
    Properties writerProperties = new Properties();
    writerProperties.putAll(this.properties);
    writerProperties.put(ConfigKeys.KAFKA_BOOTSTRAP_SERVERS, kafkaBootstrapServers);
    writerProperties.put(VeniceWriter.ENABLE_CHUNKING, chunkingEnabled);
    return new VeniceWriter<>(new VeniceProperties(writerProperties), topic, keySerializer, valueSerializer, time);
  }

  public VeniceWriter<KafkaKey, byte[]> getVeniceWriter(String topic) {
    return getVeniceWriter(topic, new KafkaKeySerializer(), new DefaultSerializer(), false, SystemTime.INSTANCE);
  }

  private void checkProperty(String key) {
    if (!properties.containsKey(key)) {
      throw new VeniceException(
          "Invalid properties for Venice writer factory. Required property: " + key + " is missing.");
    }
  }
}