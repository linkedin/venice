package com.linkedin.venice.writer;

import com.linkedin.venice.exceptions.ConfigurationException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.serialization.KafkaKeySerializer;
import com.linkedin.venice.serialization.avro.KafkaValueSerializer;
import com.linkedin.venice.utils.SslUtils;
import com.linkedin.venice.utils.VeniceProperties;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.log4j.Logger;


/**
 * Implementation of the Kafka Producer for sending messages to Kafka.
 */
public class ApacheKafkaProducer implements KafkaProducerWrapper {
  public static final String PROPERTIES_KAFKA_PREFIX = "kafka.";
  private static final Logger LOGGER = Logger.getLogger(ApacheKafkaProducer.class);

  /**
   * Mandatory Kafka SSL configs when SSL is enabled.
   */
  private static final List<String> KAFKA_SSL_MANDATORY_CONFIGS = Arrays.asList(
      CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
      SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG,
      SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG,
      SslConfigs.SSL_KEYSTORE_TYPE_CONFIG,
      SslConfigs.SSL_KEY_PASSWORD_CONFIG,
      SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG,
      SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG,
      SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG,
      SslConfigs.SSL_KEYMANAGER_ALGORITHM_CONFIG,
      SslConfigs.SSL_TRUSTMANAGER_ALGORITHM_CONFIG,
      SslConfigs.SSL_SECURE_RANDOM_IMPLEMENTATION_CONFIG
  );

  private final KafkaProducer<KafkaKey, KafkaMessageEnvelope> producer;

  public ApacheKafkaProducer(VeniceProperties props) {
    /** TODO: Consider making these default settings part of {@link VeniceWriter} or {@link KafkaProducerWrapper} */
    Properties properties = getKafkaPropertiesFromVeniceProps(props);

    // TODO : For sending control message, this is not required. Move this higher in the stack.
    ensureMandatoryProp(properties, ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaKeySerializer.class.getName());
    ensureMandatoryProp(properties, ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaValueSerializer.class.getName());

    // This is to guarantee ordering, even in the face of failures.
    ensureMandatoryProp(properties, ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1");

    // This will ensure the durability on Kafka broker side
    ensureMandatoryProp(properties, ProducerConfig.ACKS_CONFIG, "1");
    // Hard-coded retry number
    ensureMandatoryProp(properties, ProducerConfig.RETRIES_CONFIG, "100");
    // Hard-coded backoff config to be 1 sec
    ensureMandatoryProp(properties, ProducerConfig.RETRY_BACKOFF_MS_CONFIG, "1000");
    // Block if buffer is full
    ensureMandatoryProp(properties, ProducerConfig.MAX_BLOCK_MS_CONFIG, String.valueOf(Long.MAX_VALUE));

    if (properties.contains(ProducerConfig.COMPRESSION_TYPE_CONFIG)) {
      LOGGER.info("Compression type explicitly specified by config: " + properties.getProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG));
    } else {
      properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
    }

    //Setup ssl config if needed.
    if (validateAndCopyKafakaSSLConfig(props, properties)) {
      LOGGER.info("Will initialize an SSL Kafka producer");
    } else {
      LOGGER.info("Will initialize a non-SSL Kafka producer");
    }

    if (!properties.containsKey(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG)) {
      throw new ConfigurationException("Props key not found: " + PROPERTIES_KAFKA_PREFIX + ProducerConfig.BOOTSTRAP_SERVERS_CONFIG);
    }
    LOGGER.info("Constructing KafkaProducer with the following properties: " + properties.toString());

    producer = new KafkaProducer<>(properties);
    // TODO: Consider making the choice of partitioner implementation configurable
  }

  /**
   * Function which sets some needed defaults... Also bubbles up an exception in order
   * to fail fast if any calling class tries to override these defaults.
   *
   * TODO: Decide if this belongs here or higher up the call-stack
   */
  private void ensureMandatoryProp(Properties properties, String requiredConfigKey, String requiredConfigValue) {
    String actualConfigValue = properties.getProperty(requiredConfigKey);
    if (actualConfigValue == null) {
      properties.setProperty(requiredConfigKey, requiredConfigValue);
    } else if (!actualConfigValue.equals(requiredConfigValue)) {
      // We fail fast rather than attempting to use non-standard serializers
      throw new VeniceException("The Kafka Producer must use certain configuration settings in order to work properly. " +
          "requiredConfigKey: '" + requiredConfigKey +
          "', requiredConfigValue: '" + requiredConfigValue +
          "', actualConfigValue: '" + actualConfigValue + "'.");
    }
  }

  /**
   * N.B.: This is an expensive call, the result of which should be cached.
   *
   * @param topic for which we want to request the number of partitions.
   * @return the number of partitions for this topic.
   */
  public int getNumberOfPartitions(String topic) {
    // TODO: This blocks forever. We need to be able to interrupt it and throw if it "times out".
    return producer.partitionsFor(topic).size();
  }

  /**
   * Sends a message to the Kafka Producer. If everything is set up correctly, it will show up in Kafka log.
   * @param topic - The topic to be sent to.
   * @param key - The key of the message to be sent.
   * @param value - The {@link KafkaMessageEnvelope}, which acts as the Kafka value.
   * @param callback - The callback function, which will be triggered when Kafka client sends out the message.
   * */
  @Override
  public Future<RecordMetadata> sendMessage(String topic, KafkaKey key, KafkaMessageEnvelope value, int partition, Callback callback) {
    try {
      ProducerRecord<KafkaKey, KafkaMessageEnvelope> kafkaRecord = new ProducerRecord<>(topic,
          partition,
          key,
          value);
      return producer.send(kafkaRecord, callback);
    } catch (Exception e) {
      throw new VeniceException("Got an error while trying to produce message into Kafka. Topic: '" + topic +
          "', partition: " + partition, e);
    }
  }

  @Override
  public void flush() {
    if (producer != null) {
      producer.flush();
    }
  }

  @Override
  public void close(int closeTimeOutMs) {
    if (producer != null) {
      // Flush out all the messages in the producer buffer
      producer.flush();
      LOGGER.info("Flushed all the messages in producer before closing");
      producer.close(closeTimeOutMs, TimeUnit.MILLISECONDS);
    }
  }

  /**
   * This class takes in all properties that begin with "{@value #PROPERTIES_KAFKA_PREFIX}" and emits the
   * rest of the properties.
   *
   * It omits those properties that do not begin with "{@value #PROPERTIES_KAFKA_PREFIX}".
   *
   * TODO: Consider making this logic part of {@link VeniceWriter} or {@link KafkaProducerWrapper}.
  */
  private Properties getKafkaPropertiesFromVeniceProps(VeniceProperties props) {
    VeniceProperties kafkaProps = props.clipAndFilterNamespace(PROPERTIES_KAFKA_PREFIX);
    return kafkaProps.toProperties();
  }

  /**
   * This function will extract SSL related config if Kafka SSL is enabled.
   *
   * @param veniceProperties
   * @param properties
   * @return whether Kafka SSL is enabled or not
   */
  public static boolean validateAndCopyKafakaSSLConfig(VeniceProperties veniceProperties, Properties properties) {
    if (!veniceProperties.containsKey(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG)) {
      // No security protocol specified
      return false;
    }
    String kafkaProtocol = veniceProperties.getString(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG);
    if (!SslUtils.isKafkaProtocolValid(kafkaProtocol)) {
      throw new VeniceException("Invalid Kafka protocol specified: " + kafkaProtocol);
    }
    if (!SslUtils.isKafkaSSLProtocol(kafkaProtocol)) {
      // TLS/SSL is not enabled
      return false;
    }
    // Since SSL is enabled, the following configs are mandatory
    KAFKA_SSL_MANDATORY_CONFIGS.forEach( config -> {
      if (!veniceProperties.containsKey(config)) {
        throw new VeniceException(config + " is required when Kafka SSL is enabled");
      }
      properties.setProperty(config, veniceProperties.getString(config));
    });
    return true;
  }
}
