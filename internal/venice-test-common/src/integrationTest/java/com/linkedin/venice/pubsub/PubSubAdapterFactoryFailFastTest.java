package com.linkedin.venice.pubsub;

import static com.linkedin.venice.ConfigKeys.CLUSTER_NAME;
import static com.linkedin.venice.ConfigKeys.INGESTION_USE_DA_VINCI_CLIENT;
import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.ConfigKeys.PUBSUB_ADAPTER_FACTORY_KAFKA_FALLBACK_ENABLED;
import static com.linkedin.venice.ConfigKeys.PUBSUB_ADMIN_ADAPTER_FACTORY_CLASS;
import static com.linkedin.venice.ConfigKeys.PUBSUB_CONSUMER_ADAPTER_FACTORY_CLASS;
import static com.linkedin.venice.ConfigKeys.PUBSUB_PRODUCER_ADAPTER_FACTORY_CLASS;
import static com.linkedin.venice.ConfigKeys.ZOOKEEPER_ADDRESS;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.pubsub.adapter.kafka.admin.ApacheKafkaAdminAdapterFactory;
import com.linkedin.venice.pubsub.adapter.kafka.consumer.ApacheKafkaConsumerAdapterFactory;
import com.linkedin.venice.pubsub.adapter.kafka.producer.ApacheKafkaProducerAdapterFactory;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.Properties;
import org.testng.annotations.Test;


/**
 * End-to-end coverage for the opt-in fail-fast behavior of the pub-sub adapter factories, exercised
 * through a real production config object ({@link VeniceServerConfig}) rather than the factory in
 * isolation. {@link VeniceServerConfig} eagerly constructs a {@link PubSubClientsFactory} from its
 * properties, so it is representative of how a Venice component resolves its pub-sub clients at
 * startup.
 * <p>
 * The behavior is opt-in via {@link com.linkedin.venice.ConfigKeys#PUBSUB_ADAPTER_FACTORY_KAFKA_FALLBACK_ENABLED}:
 * by default the config keeps working (Apache Kafka fallback); with the fallback disabled it fails
 * fast when the adapter factory class is not configured.
 */
public class PubSubAdapterFactoryFailFastTest {
  private static Properties baseServerProperties() {
    Properties props = new Properties();
    props.setProperty(CLUSTER_NAME, "test_cluster");
    props.setProperty(ZOOKEEPER_ADDRESS, "localhost:2181");
    props.setProperty(KAFKA_BOOTSTRAP_SERVERS, "localhost:9092");
    props.setProperty(INGESTION_USE_DA_VINCI_CLIENT, "true");
    return props;
  }

  @Test
  public void serverConfigDefaultsToApacheKafkaWhenFallbackNotConfigured() {
    VeniceServerConfig config = new VeniceServerConfig(new VeniceProperties(baseServerProperties()));
    assertNotNull(config.getPubSubClientsFactory());
    assertNotNull(config.getPubSubClientsFactory().getProducerAdapterFactory());
  }

  @Test
  public void serverConfigFailsFastWhenFallbackDisabledAndFactoryClassMissing() {
    Properties props = baseServerProperties();
    props.setProperty(PUBSUB_ADAPTER_FACTORY_KAFKA_FALLBACK_ENABLED, "false");

    VeniceException e = expectThrows(VeniceException.class, () -> new VeniceServerConfig(new VeniceProperties(props)));
    assertTrue(
        e.getMessage().contains(PUBSUB_PRODUCER_ADAPTER_FACTORY_CLASS),
        "Expected fail-fast message to name the missing factory-class config but was: " + e.getMessage());
    assertTrue(
        e.getMessage().contains(PUBSUB_ADAPTER_FACTORY_KAFKA_FALLBACK_ENABLED),
        "Expected fail-fast message to name the fallback config key but was: " + e.getMessage());
  }

  @Test
  public void serverConfigSucceedsWhenFallbackDisabledButFactoryClassesProvided() {
    Properties props = baseServerProperties();
    props.setProperty(PUBSUB_ADAPTER_FACTORY_KAFKA_FALLBACK_ENABLED, "false");
    props.setProperty(PUBSUB_PRODUCER_ADAPTER_FACTORY_CLASS, ApacheKafkaProducerAdapterFactory.class.getName());
    props.setProperty(PUBSUB_CONSUMER_ADAPTER_FACTORY_CLASS, ApacheKafkaConsumerAdapterFactory.class.getName());
    props.setProperty(PUBSUB_ADMIN_ADAPTER_FACTORY_CLASS, ApacheKafkaAdminAdapterFactory.class.getName());

    VeniceServerConfig config = new VeniceServerConfig(new VeniceProperties(props));
    assertNotNull(config.getPubSubClientsFactory());
    assertNotNull(config.getPubSubClientsFactory().getProducerAdapterFactory());
  }
}
