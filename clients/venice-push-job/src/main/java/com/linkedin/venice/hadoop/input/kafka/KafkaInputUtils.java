package com.linkedin.venice.hadoop.input.kafka;

import static com.linkedin.venice.hadoop.VenicePushJob.KAFKA_INPUT_BROKER_URL;
import static com.linkedin.venice.hadoop.VenicePushJob.SSL_CONFIGURATOR_CLASS_CONFIG;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.ssl.SSLConfigurator;
import com.linkedin.venice.hadoop.ssl.UserCredentialsFactory;
import com.linkedin.venice.hadoop.utils.HadoopUtils;
import com.linkedin.venice.kafka.KafkaClientFactory;
import com.linkedin.venice.kafka.consumer.KafkaConsumerFactoryImpl;
import com.linkedin.venice.utils.KafkaSSLUtils;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.IOException;
import java.util.Properties;
import org.apache.hadoop.mapred.JobConf;
import org.apache.kafka.clients.CommonClientConfigs;


public class KafkaInputUtils {
  public static KafkaClientFactory getConsumerFactory(JobConf config) {
    Properties sslProps = null;
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
        KafkaSSLUtils.validateAndCopyKafkaSSLConfig(veniceProperties, consumerFactoryProperties);
      } catch (IOException e) {
        throw new VeniceException("Could not get user credential for job:" + config.getJobName(), e);
      }
    }

    /**
     * Use a large receive buffer size: 4MB since Kafka re-push could consume remotely.
     */
    consumerFactoryProperties.setProperty(CommonClientConfigs.RECEIVE_BUFFER_CONFIG, Long.toString(4 * 1024 * 1024));
    consumerFactoryProperties
        .setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, config.get(KAFKA_INPUT_BROKER_URL));

    return new KafkaConsumerFactoryImpl(new VeniceProperties(consumerFactoryProperties));
  }
}
