package com.linkedin.venice.hadoop.input.kafka;

import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.ConfigKeys.KAFKA_CONFIG_PREFIX;
import static com.linkedin.venice.ConfigKeys.PUBSUB_BROKER_ADDRESS;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_INPUT_BROKER_URL;
import static com.linkedin.venice.vpj.VenicePushJobConstants.NEWER_KME_SCHEMAS_PREFIX;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SSL_CONFIGURATOR_CLASS_CONFIG;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SYSTEM_SCHEMA_READER_ENABLED;

import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.compression.CompressorFactory;
import com.linkedin.venice.compression.VeniceCompressor;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.ssl.SSLConfigurator;
import com.linkedin.venice.hadoop.ssl.UserCredentialsFactory;
import com.linkedin.venice.hadoop.utils.HadoopUtils;
import com.linkedin.venice.schema.SchemaReader;
import com.linkedin.venice.serialization.avro.KafkaValueSerializer;
import com.linkedin.venice.serialization.avro.OptimizedKafkaValueSerializer;
import com.linkedin.venice.utils.ByteUtils;
import com.linkedin.venice.utils.DictionaryUtils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.vpj.VenicePushJobConstants;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import org.apache.hadoop.mapred.JobConf;
import org.apache.kafka.clients.CommonClientConfigs;


public class KafkaInputUtils {
  private static Properties sslProps = null;

  /**
   * Extracts and prepares the Kafka consumer properties for a Venice input job.
   *
   * <p>This method:
   * <ul>
   *   <li>Copies all Hadoop job configurations into a {@link Properties} object.</li>
   *   <li>If an SSL configurator is specified, applies SSL settings and merges SSL properties into the consumer properties.</li>
   *   <li>Clips and merges any {@link VenicePushJobConstants#KIF_RECORD_READER_KAFKA_CONFIG_PREFIX} prefixed properties into the consumer properties.</li>
   *   <li>Sets a large receive buffer size (4MB) to support remote Kafka re-push scenarios.</li>
   *   <li>Sets the PubSub bootstrap server address</li>
   * </ul>
   *
   * @param config the Hadoop {@link JobConf} containing the job configurations
   * @return a {@link VeniceProperties} object containing the prepared Kafka consumer properties
   * @throws VeniceException if SSL configuration setup fails
   */

  public static VeniceProperties getConsumerProperties(JobConf config, Properties overrideProperties) {
    Properties allProperties = HadoopUtils.getProps(config);
    Properties consumerProperties = new Properties();
    consumerProperties.putAll(allProperties); // manually copy all properties
    if (config.get(SSL_CONFIGURATOR_CLASS_CONFIG) != null) {
      SSLConfigurator configurator = SSLConfigurator.getSSLConfigurator(config.get(SSL_CONFIGURATOR_CLASS_CONFIG));
      try {
        sslProps = configurator.setupSSLConfig(allProperties, UserCredentialsFactory.getHadoopUserCredentials());
        // Add back SSL properties to the consumer properties; sslProps may have overridden some of the properties or
        // may return only SSL related properties.
        consumerProperties.putAll(sslProps);
      } catch (IOException e) {
        throw new VeniceException("Could not get user credential for job:" + config.getJobName(), e);
      }
    }
    VeniceProperties veniceProperties = new VeniceProperties(allProperties);
    // Drop the prefixes for some properties and add them back the consumer properties.
    consumerProperties.putAll(
        veniceProperties.clipAndFilterNamespace(VenicePushJobConstants.KIF_RECORD_READER_KAFKA_CONFIG_PREFIX)
            .toProperties());
    /**
     * Use a large receive buffer size: 4MB since Kafka re-push could consume remotely.
     * TODO: Remove hardcoded buffer size and pass it down from the job config.
     */
    consumerProperties
        .setProperty(KAFKA_CONFIG_PREFIX + CommonClientConfigs.RECEIVE_BUFFER_CONFIG, Long.toString(4 * 1024 * 1024));
    consumerProperties.setProperty(KAFKA_BOOTSTRAP_SERVERS, config.get(KAFKA_INPUT_BROKER_URL));
    consumerProperties.setProperty(PUBSUB_BROKER_ADDRESS, config.get(KAFKA_INPUT_BROKER_URL));

    // Add any override properties to the consumer properties.
    if (overrideProperties != null) {
      consumerProperties.putAll(overrideProperties);
    }
    return new VeniceProperties(consumerProperties);
  }

  public static VeniceProperties getConsumerProperties(JobConf config) {
    return getConsumerProperties(config, null);
  }

  public static KafkaValueSerializer getKafkaValueSerializer(JobConf config) {
    KafkaValueSerializer kafkaValueSerializer = new OptimizedKafkaValueSerializer();
    boolean isSchemaReaderEnabled = Boolean.parseBoolean(config.get(SYSTEM_SCHEMA_READER_ENABLED, "false"));
    if (isSchemaReaderEnabled) {

      Map<String, String> newerKMESchemaStrings = config.getPropsWithPrefix(NEWER_KME_SCHEMAS_PREFIX);
      Map<Integer, String> newerIdToSchemas = newerKMESchemaStrings.entrySet()
          .stream()
          .collect(Collectors.toMap(e -> Integer.parseInt(e.getKey()), Map.Entry::getValue));

      SchemaReader schemaReader = new KMESchemaReaderForKafkaInputFormat(newerIdToSchemas);
      kafkaValueSerializer.setSchemaReader(schemaReader);
    }
    return kafkaValueSerializer;
  }

  public static VeniceCompressor getCompressor(
      CompressorFactory compressorFactory,
      CompressionStrategy strategy,
      String kafkaUrl,
      String topic,
      VeniceProperties properties) {
    if (strategy.equals(CompressionStrategy.ZSTD_WITH_DICT)) {
      Properties props = properties.toProperties();
      props.setProperty(KAFKA_BOOTSTRAP_SERVERS, kafkaUrl);
      ByteBuffer dict = DictionaryUtils.readDictionaryFromKafka(topic, new VeniceProperties(props));
      return compressorFactory
          .createVersionSpecificCompressorIfNotExist(strategy, topic, ByteUtils.extractByteArray(dict));
    }
    return compressorFactory.getCompressor(strategy);
  }

  /**
   * Puts a Map of schema ID to schema string into Properties using the specified prefix.
   * Each entry is stored as: prefix + schemaId = schemaString
   *
   * @param schemaMap the Map containing schema ID to schema string mappings
   */
  public static Map<String, String> putSchemaMapIntoProperties(Map<Integer, String> schemaMap) {

    Map<String, String> schemaMapWithPrefix = new HashMap<>();
    if (schemaMap == null || schemaMap.isEmpty()) {
      return schemaMapWithPrefix;
    }
    for (Map.Entry<Integer, String> entry: schemaMap.entrySet()) {
      String schemaWithPrefix = NEWER_KME_SCHEMAS_PREFIX + entry.getKey();
      schemaMapWithPrefix.put(schemaWithPrefix, entry.getValue());
    }
    return schemaMapWithPrefix;
  }

}
