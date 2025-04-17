package com.linkedin.venice.hadoop.input.kafka;

import static com.linkedin.venice.CommonConfigKeys.SSL_FACTORY_CLASS_NAME;
import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.ConfigKeys.PUBSUB_BROKER_ADDRESS;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_INPUT_BROKER_URL;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SSL_CONFIGURATOR_CLASS_CONFIG;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SYSTEM_SCHEMA_CLUSTER_D2_SERVICE_NAME;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SYSTEM_SCHEMA_CLUSTER_D2_ZK_HOST;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SYSTEM_SCHEMA_READER_ENABLED;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.d2.balancer.D2ClientBuilder;
import com.linkedin.venice.D2.D2ClientUtils;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.compression.CompressorFactory;
import com.linkedin.venice.compression.VeniceCompressor;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.ssl.SSLConfigurator;
import com.linkedin.venice.hadoop.ssl.UserCredentialsFactory;
import com.linkedin.venice.hadoop.utils.HadoopUtils;
import com.linkedin.venice.pubsub.adapter.kafka.producer.ApacheKafkaProducerConfig;
import com.linkedin.venice.schema.SchemaReader;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.KafkaValueSerializer;
import com.linkedin.venice.serialization.avro.OptimizedKafkaValueSerializer;
import com.linkedin.venice.utils.ByteUtils;
import com.linkedin.venice.utils.DictionaryUtils;
import com.linkedin.venice.utils.SslUtils;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.Properties;
import org.apache.hadoop.mapred.JobConf;
import org.apache.kafka.clients.CommonClientConfigs;


public class KafkaInputUtils {
  private static Properties sslProps = null;

  public static VeniceProperties getConsumerProperties(JobConf config) {
    Properties consumerFactoryProperties = new Properties();
    if (config.get(SSL_CONFIGURATOR_CLASS_CONFIG) != null) {
      SSLConfigurator configurator = SSLConfigurator.getSSLConfigurator(config.get(SSL_CONFIGURATOR_CLASS_CONFIG));
      try {
        sslProps = configurator
            .setupSSLConfig(HadoopUtils.getProps(config), UserCredentialsFactory.getHadoopUserCredentials());
        VeniceProperties veniceProperties = new VeniceProperties(sslProps);
        // Copy the pass-through Kafka properties
        consumerFactoryProperties.putAll(
            veniceProperties.clipAndFilterNamespace(KafkaInputRecordReader.KIF_RECORD_READER_KAFKA_CONFIG_PREFIX)
                .toProperties());
        // Copy the mandatory ssl configs
        consumerFactoryProperties.putAll(veniceProperties.getAsMap());
      } catch (IOException e) {
        throw new VeniceException("Could not get user credential for job:" + config.getJobName(), e);
      }
    }

    /**
     * Use a large receive buffer size: 4MB since Kafka re-push could consume remotely.
     * TODO(sushantmane): Remove hardcoded buffer size and pass it down from the job config.
     */
    consumerFactoryProperties.setProperty(CommonClientConfigs.RECEIVE_BUFFER_CONFIG, Long.toString(4 * 1024 * 1024));
    consumerFactoryProperties.setProperty(KAFKA_BOOTSTRAP_SERVERS, config.get(KAFKA_INPUT_BROKER_URL));
    consumerFactoryProperties.setProperty(PUBSUB_BROKER_ADDRESS, config.get(KAFKA_INPUT_BROKER_URL));

    ApacheKafkaProducerConfig.copyKafkaSASLProperties(HadoopUtils.getProps(config), consumerFactoryProperties, true);

    return new VeniceProperties(consumerFactoryProperties);
  }

  public static KafkaValueSerializer getKafkaValueSerializer(JobConf config) {
    KafkaValueSerializer kafkaValueSerializer = new OptimizedKafkaValueSerializer();
    boolean isSchemaReaderEnabled = Boolean.parseBoolean(config.get(SYSTEM_SCHEMA_READER_ENABLED, "false"));
    if (isSchemaReaderEnabled) {
      Optional<SSLFactory> sslFactory = Optional.empty();
      if (sslProps != null) {
        String sslFactoryClassName = config.get(SSL_FACTORY_CLASS_NAME);
        sslFactory = Optional.of(SslUtils.getSSLFactory(sslProps, sslFactoryClassName));
      }
      String systemSchemaClusterD2ZKHost = config.get(SYSTEM_SCHEMA_CLUSTER_D2_ZK_HOST);
      D2Client d2Client = new D2ClientBuilder().setZkHosts(systemSchemaClusterD2ZKHost)
          .setSSLContext(sslFactory.map(SSLFactory::getSSLContext).orElse(null))
          .setIsSSLEnabled(sslFactory.isPresent())
          .setSSLParameters(sslFactory.map(SSLFactory::getSSLParameters).orElse(null))
          .build();
      D2ClientUtils.startClient(d2Client);

      String systemSchemaClusterD2ServiceName = config.get(SYSTEM_SCHEMA_CLUSTER_D2_SERVICE_NAME);
      String kafkaMessageEnvelopeSystemStoreName = AvroProtocolDefinition.KAFKA_MESSAGE_ENVELOPE.getSystemStoreName();
      ClientConfig kafkaMessageEnvelopeClientConfig =
          ClientConfig.defaultGenericClientConfig(kafkaMessageEnvelopeSystemStoreName);
      kafkaMessageEnvelopeClientConfig.setD2ServiceName(systemSchemaClusterD2ServiceName);
      kafkaMessageEnvelopeClientConfig.setD2Client(d2Client);
      SchemaReader kafkaMessageEnvelopeSchemaReader = ClientFactory.getSchemaReader(kafkaMessageEnvelopeClientConfig);
      kafkaValueSerializer.setSchemaReader(kafkaMessageEnvelopeSchemaReader);
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

}
