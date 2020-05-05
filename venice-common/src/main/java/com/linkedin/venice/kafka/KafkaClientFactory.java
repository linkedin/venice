package com.linkedin.venice.kafka;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.kafka.admin.KafkaAdminWrapper;
import com.linkedin.venice.kafka.consumer.ApacheKafkaConsumer;
import com.linkedin.venice.kafka.consumer.KafkaConsumerWrapper;
import com.linkedin.venice.utils.ReflectUtils;
import java.util.Properties;
import org.apache.kafka.clients.consumer.KafkaConsumer;


public abstract class KafkaClientFactory {
  public KafkaConsumerWrapper getConsumer(Properties props) {
    return new ApacheKafkaConsumer(setupSSL(props));
  }

  public <K, V> KafkaConsumer<K, V> getKafkaConsumer(Properties properties){
    return new KafkaConsumer<>(setupSSL(properties));
  }

  public KafkaAdminWrapper getKafkaAdminClient() {
    KafkaAdminWrapper adminWrapper = ReflectUtils.callConstructor(
        ReflectUtils.loadClass(getKafkaAdminClass()),
        new Class[0],
        new Object[0]
    );
    Properties properties = setupSSL(new Properties());
    properties.setProperty(ConfigKeys.KAFKA_ZK_ADDRESS, getKafkaZkAddress());
    adminWrapper.initialize(properties);
    return adminWrapper;
  }

  /**
   * Setup essential ssl related configuration by putting all ssl properties of this factory into the given properties.
   */
  public abstract Properties setupSSL(Properties properties);

  abstract protected String getKafkaAdminClass();

  abstract protected String getKafkaZkAddress();
}
