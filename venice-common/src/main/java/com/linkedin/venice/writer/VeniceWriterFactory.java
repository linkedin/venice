package com.linkedin.venice.writer;

import com.linkedin.security.ssl.access.control.SSLEngineComponentFactoryImpl;
import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.kafka.validation.checksum.CheckSumType;
import com.linkedin.venice.serialization.DefaultSerializer;
import com.linkedin.venice.utils.SslUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.VeniceProperties;

import java.util.Properties;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SslConfigs;

/**
 *
 */
public class VeniceWriterFactory {
  private static final VeniceWriterFactory FACTORY = new VeniceWriterFactory();

  private VeniceWriterFactory() {
    // singleton
  }

  public static VeniceWriterFactory get() {
    return FACTORY;
  }

  public VeniceWriter<byte[], byte[]> getBasicVeniceWriter(String kafkaBootstrapServers, String topicName, Time time) {
    Properties props = new Properties();
    props.put(ConfigKeys.KAFKA_BOOTSTRAP_SERVERS, kafkaBootstrapServers);
    props.put(VeniceWriter.CHECK_SUM_TYPE, CheckSumType.NONE.name());
    VeniceProperties veniceWriterProperties = new VeniceProperties(props);
    return new VeniceWriter<>(veniceWriterProperties, topicName, new DefaultSerializer(), new DefaultSerializer(), time);
  }

  public VeniceWriter<byte[], byte[]> getSslVeniceWriter(String kafkaBootstrapServers, String topicName, Time time,String securityProtocol, SslConfigs sslConfigs){
    // TODO Create ssl enabled venice writer for admin topic used by parent controller.
    return null;
  }
}
