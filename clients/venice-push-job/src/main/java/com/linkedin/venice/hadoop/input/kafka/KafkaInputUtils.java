package com.linkedin.venice.hadoop.input.kafka;

import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.ConfigKeys.KAFKA_CONFIG_PREFIX;
import static com.linkedin.venice.ConfigKeys.PUBSUB_BROKER_ADDRESS;
import static com.linkedin.venice.vpj.VenicePushJobConstants.NEWER_KME_SCHEMAS_PREFIX;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SSL_CONFIGURATOR_CLASS_CONFIG;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SYSTEM_SCHEMA_READER_ENABLED;
import static com.linkedin.venice.vpj.VenicePushJobConstants.VENICE_REPUSH_SOURCE_PUBSUB_BROKER;

import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.compression.CompressorFactory;
import com.linkedin.venice.compression.VeniceCompressor;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.ssl.SSLConfigurator;
import com.linkedin.venice.hadoop.ssl.UserCredentialsFactory;
import com.linkedin.venice.hadoop.utils.HadoopUtils;
import com.linkedin.venice.pubsub.api.PubSubMessageDeserializer;
import com.linkedin.venice.schema.KmeSchemaReader;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.apache.hadoop.mapred.JobConf;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class KafkaInputUtils {
  private static final Logger LOGGER = LogManager.getLogger(KafkaInputUtils.class);
  private static final AtomicBoolean SCHEMA_READER_DISABLED_WARNED = new AtomicBoolean(false);
  private static final AtomicBoolean SCHEMA_READER_EMPTY_BROADCAST_WARNED = new AtomicBoolean(false);
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
    String repushSourcePubsubBroker = config.get(VENICE_REPUSH_SOURCE_PUBSUB_BROKER);
    if (repushSourcePubsubBroker == null) {
      throw new VeniceException(VENICE_REPUSH_SOURCE_PUBSUB_BROKER + " is required but was not found in job config");
    }
    consumerProperties.setProperty(PUBSUB_BROKER_ADDRESS, repushSourcePubsubBroker);

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

      SchemaReader schemaReader = new KmeSchemaReader(newerIdToSchemas);
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
      /*
       * Forward-compat: feed the dictionary fetch a deserializer wired with the same
       * KmeSchemaReader that getKafkaValueSerializer builds for the actual
       * record-read path. If the SoP control-message record on the source VT lands without a
       * vtp header (e.g. a writer regression like the empty-headers DoL stamp), the reader
       * still resolves an unknown KME protocol version via the controller-broadcast schemas
       * instead of throwing 'Received Protocol Version N which is not supported'.
       */
      PubSubMessageDeserializer deserializer = buildSchemaAwareDeserializer(properties);
      ByteBuffer dict = DictionaryUtils.readDictionaryFromKafka(topic, new VeniceProperties(props), deserializer);
      return compressorFactory
          .createVersionSpecificCompressorIfNotExist(strategy, topic, ByteUtils.extractByteArray(dict));
    }
    return compressorFactory.getCompressor(strategy);
  }

  /**
   * Build a {@link PubSubMessageDeserializer} (non-optimized variant) for paths that fetch a
   * single record (e.g., dictionary fetch). See {@link #doBuildSchemaAwareDeserializer} for
   * fallback semantics.
   */
  public static PubSubMessageDeserializer buildSchemaAwareDeserializer(VeniceProperties properties) {
    return doBuildSchemaAwareDeserializer(properties, /* optimized */ false);
  }

  /**
   * Build a {@link PubSubMessageDeserializer} backed by {@link OptimizedKafkaValueSerializer}
   * (reused decoders) for hot read paths like Spark VPJ record readers. See
   * {@link #doBuildSchemaAwareDeserializer} for fallback semantics.
   */
  public static PubSubMessageDeserializer buildSchemaAwareOptimizedDeserializer(VeniceProperties properties) {
    return doBuildSchemaAwareDeserializer(properties, /* optimized */ true);
  }

  /**
   * Builds a {@link PubSubMessageDeserializer} whose value serializer is wired with a
   * {@link KmeSchemaReader} populated from the {@code NEWER_KME_SCHEMAS_PREFIX} entries in
   * {@code properties} (the VPJ driver broadcasts these to executors via Spark/MR conf).
   *
   * <p>Graceful fallback path: returns a jar-only deserializer (default or optimized) when
   * either {@code SYSTEM_SCHEMA_READER_ENABLED} is false or the broadcast schemas are absent.
   * The on-wire {@code vtp} header bootstrap inside {@link PubSubMessageDeserializer#deserialize}
   * still applies on the fallback path. Each fallback condition logs at most once per JVM (via
   * {@link AtomicBoolean} gates) to avoid spamming Spark executor logs on older configs that
   * construct a new deserializer per partition.
   */
  private static PubSubMessageDeserializer doBuildSchemaAwareDeserializer(
      VeniceProperties properties,
      boolean optimized) {
    boolean isSchemaReaderEnabled = properties.getBoolean(SYSTEM_SCHEMA_READER_ENABLED, false);
    if (!isSchemaReaderEnabled) {
      if (SCHEMA_READER_DISABLED_WARNED.compareAndSet(false, true)) {
        LOGGER.warn(
            "{} is false; falling back to the jar-only KME deserializer. The on-wire vtp header bootstrap "
                + "still applies. Set {}=true on the job conf so the VPJ driver broadcasts newer.kme.schemas.* "
                + "to executors and consumers can resolve unknown KME protocol versions without depending on vtp. "
                + "(This warning is logged once per JVM.)",
            SYSTEM_SCHEMA_READER_ENABLED,
            SYSTEM_SCHEMA_READER_ENABLED);
      }
      return optimized
          ? PubSubMessageDeserializer.createOptimizedDeserializer()
          : PubSubMessageDeserializer.createDefaultDeserializer();
    }
    Properties clipped = properties.clipAndFilterNamespace(NEWER_KME_SCHEMAS_PREFIX).toProperties();
    if (clipped.isEmpty()) {
      if (SCHEMA_READER_EMPTY_BROADCAST_WARNED.compareAndSet(false, true)) {
        LOGGER.warn(
            "{} is true but no {}* entries are present on the job conf; falling back to the jar-only KME "
                + "deserializer. The VPJ driver likely failed to populate the broadcast - confirm "
                + "validateAndFetchNewKafkaMessageEnvelopeSchemas ran successfully against the controller. "
                + "(This warning is logged once per JVM.)",
            SYSTEM_SCHEMA_READER_ENABLED,
            NEWER_KME_SCHEMAS_PREFIX);
      }
      return optimized
          ? PubSubMessageDeserializer.createOptimizedDeserializer()
          : PubSubMessageDeserializer.createDefaultDeserializer();
    }
    Map<Integer, String> newerIdToSchemas = new HashMap<>();
    for (Map.Entry<Object, Object> entry: clipped.entrySet()) {
      newerIdToSchemas.put(Integer.parseInt((String) entry.getKey()), (String) entry.getValue());
    }
    SchemaReader schemaReader = new KmeSchemaReader(newerIdToSchemas);
    return optimized
        ? PubSubMessageDeserializer.createOptimizedWithSchemaReader(schemaReader)
        : PubSubMessageDeserializer.createWithSchemaReader(schemaReader);
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
