package com.linkedin.venice.pubsub.adapter.kafka.producer;

import static com.linkedin.venice.pubsub.adapter.kafka.producer.ApacheKafkaProducerConfig.KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.pubsub.adapter.kafka.producer.ApacheKafkaProducerConfig.SSL_KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.pubsub.adapter.kafka.producer.ApacheKafkaProducerConfig.SSL_TO_KAFKA_LEGACY;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.pubsub.PubSubConstants;
import com.linkedin.venice.pubsub.api.PubSubProducerAdapterContext;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.Properties;
import java.util.function.BiConsumer;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class ApacheKafkaProducerConfigTest {
  private static final String KAFKA_BROKER_ADDR = "kafka.broker.com:8181";
  private static final String PRODUCER_NAME = "sender-store_v1";

  private static final String SASL_JAAS_CONFIG =
      "org.apache.kafka.common.security.plain.PlainLoginModule required " + "username=\"foo\" password=\"bar\"\n";

  private static final String SASL_MECHANISM = "PLAIN";

  @Test
  public void testConfiguratorThrowsAnExceptionWhenBrokerAddressIsMissing() {
    VeniceProperties veniceProperties = VeniceProperties.empty();
    PubSubProducerAdapterContext context = mock(PubSubProducerAdapterContext.class);
    when(context.getBrokerAddress()).thenReturn(null);
    when(context.getVeniceProperties()).thenReturn(veniceProperties);
    when(context.shouldValidateProducerConfigStrictly()).thenReturn(true);
    VeniceException e = expectThrows(VeniceException.class, () -> new ApacheKafkaProducerConfig(context));
    assertTrue(e.getMessage().contains("Required property: kafka.bootstrap.servers is missing"));
  }

  @Test
  public void testGetBrokerAddress() {
    // broker address from props should be used
    Properties props = new Properties();
    props.put(KAFKA_BOOTSTRAP_SERVERS, KAFKA_BROKER_ADDR);
    PubSubProducerAdapterContext context = mock(PubSubProducerAdapterContext.class);
    when(context.getBrokerAddress()).thenReturn(null);
    when(context.getVeniceProperties()).thenReturn(new VeniceProperties(props));
    when(context.shouldValidateProducerConfigStrictly()).thenReturn(true);
    ApacheKafkaProducerConfig producerConfig = new ApacheKafkaProducerConfig(context);
    assertNotNull(producerConfig);
    assertEquals(producerConfig.getBrokerAddress(), KAFKA_BROKER_ADDR);
    assertFalse(producerConfig.getProducerProperties().containsKey(ProducerConfig.CLIENT_ID_CONFIG));

    String overriddenBrokerAddress = "target.broker.com:8181";
    context = mock(PubSubProducerAdapterContext.class);
    when(context.getBrokerAddress()).thenReturn(overriddenBrokerAddress);
    when(context.getVeniceProperties()).thenReturn(new VeniceProperties(props));
    when(context.shouldValidateProducerConfigStrictly()).thenReturn(true);
    when(context.getProducerName()).thenReturn(PRODUCER_NAME);
    ApacheKafkaProducerConfig producerConfig1 = new ApacheKafkaProducerConfig(context);
    // broker address from props should be used
    assertEquals(producerConfig1.getBrokerAddress(), overriddenBrokerAddress);
    assertEquals(producerConfig1.getProducerProperties().getProperty(ProducerConfig.CLIENT_ID_CONFIG), PRODUCER_NAME);
  }

  @Test
  public void testGetBrokerAddressReturnsSslAddrIfKafkaSslIsEnabled() {
    // broker address from props should be used
    Properties props = new Properties();
    props.put(SSL_TO_KAFKA_LEGACY, true);
    props.put(KAFKA_BOOTSTRAP_SERVERS, KAFKA_BROKER_ADDR);
    props.put(SSL_KAFKA_BOOTSTRAP_SERVERS, "ssl.kafka.broker.com:8182");
    PubSubProducerAdapterContext context = mock(PubSubProducerAdapterContext.class);
    when(context.getBrokerAddress()).thenReturn(null);
    when(context.getVeniceProperties()).thenReturn(new VeniceProperties(props));
    when(context.shouldValidateProducerConfigStrictly()).thenReturn(true);
    assertEquals(new ApacheKafkaProducerConfig(context).getBrokerAddress(), "ssl.kafka.broker.com:8182");
  }

  @Test
  public void testSaslConfiguration() {
    // broker address from props should be used
    Properties props = new Properties();
    props.put(SSL_TO_KAFKA_LEGACY, true);
    props.put(KAFKA_BOOTSTRAP_SERVERS, KAFKA_BROKER_ADDR);
    props.put(SSL_KAFKA_BOOTSTRAP_SERVERS, "ssl.kafka.broker.com:8182");
    props.put("kafka.sasl.jaas.config", SASL_JAAS_CONFIG);
    props.put("kafka.sasl.mechanism", SASL_MECHANISM);
    props.put("kafka.security.protocol", "SASL_SSL");

    PubSubProducerAdapterContext context = mock(PubSubProducerAdapterContext.class);
    when(context.getBrokerAddress()).thenReturn(null);
    when(context.getVeniceProperties()).thenReturn(new VeniceProperties(props));
    when(context.shouldValidateProducerConfigStrictly()).thenReturn(true);
    ApacheKafkaProducerConfig apacheKafkaProducerConfig = new ApacheKafkaProducerConfig(context);
    Properties producerProperties = apacheKafkaProducerConfig.getProducerProperties();
    assertEquals(SASL_JAAS_CONFIG, producerProperties.get("sasl.jaas.config"));
    assertEquals(SASL_MECHANISM, producerProperties.get("sasl.mechanism"));
    assertEquals("SASL_SSL", producerProperties.get("security.protocol"));
  }

  @DataProvider(name = "stripPrefix")
  public static Object[][] stripPrefix() {
    return new Object[][] { { true }, { false } };
  }

  @Test(dataProvider = "stripPrefix")
  public void testCopySaslConfiguration(boolean stripPrefix) {
    Properties config = new Properties();
    config.put("kafka.sasl.jaas.config", SASL_JAAS_CONFIG);
    config.put("kafka.sasl.mechanism", SASL_MECHANISM);
    config.put("kafka.security.protocol", "SASL_SSL");

    testCopy(
        stripPrefix,
        config,
        (input, output) -> ApacheKafkaProducerConfig
            .copyKafkaSASLProperties(new VeniceProperties(input), output, stripPrefix));

    testCopy(
        stripPrefix,
        config,
        (input, output) -> ApacheKafkaProducerConfig.copyKafkaSASLProperties(input, output, stripPrefix));
  }

  private static void testCopy(boolean stripPrefix, Properties input, BiConsumer<Properties, Properties> copy) {
    Properties output = new Properties();
    copy.accept(input, output);
    if (stripPrefix) {
      assertEquals(SASL_JAAS_CONFIG, output.get("sasl.jaas.config"));
      assertEquals(SASL_MECHANISM, output.get("sasl.mechanism"));
      assertEquals("SASL_SSL", output.get("security.protocol"));
    } else {
      assertEquals(SASL_JAAS_CONFIG, output.get("kafka.sasl.jaas.config"));
      assertEquals(SASL_MECHANISM, output.get("kafka.sasl.mechanism"));
      assertEquals("SASL_SSL", output.get("kafka.security.protocol"));
    }
  }

  @Test
  public void testAddHighThroughputDefaultsCanSetProperConfigs() {
    PubSubProducerAdapterContext context;

    // should not set batch size and linger ms if high throughput defaults are not enabled
    context = new PubSubProducerAdapterContext.Builder().setVeniceProperties(VeniceProperties.empty())
        .setBrokerAddress(KAFKA_BROKER_ADDR)
        .setShouldValidateProducerConfigStrictly(false)
        .setProducerName(PRODUCER_NAME)
        .build();
    ApacheKafkaProducerConfig apacheKafkaProducerConfig = new ApacheKafkaProducerConfig(context);
    Properties actualProps = apacheKafkaProducerConfig.getProducerProperties();
    assertFalse(actualProps.containsKey(ProducerConfig.BATCH_SIZE_CONFIG));
    assertFalse(actualProps.containsKey(ProducerConfig.LINGER_MS_CONFIG));

    Properties veniceProperties = new Properties();

    // should set batch size and linger ms if high throughput defaults are enabled and batch size and linger ms are not
    // set
    veniceProperties.put(PubSubConstants.PUBSUB_PRODUCER_USE_HIGH_THROUGHPUT_DEFAULTS, "true");
    context = new PubSubProducerAdapterContext.Builder().setVeniceProperties(new VeniceProperties(veniceProperties))
        .setBrokerAddress(KAFKA_BROKER_ADDR)
        .setShouldValidateProducerConfigStrictly(false)
        .setProducerName(PRODUCER_NAME)
        .build();
    ApacheKafkaProducerConfig apacheKafkaProducerConfig1 = new ApacheKafkaProducerConfig(context);
    Properties actualProps1 = apacheKafkaProducerConfig1.getProducerProperties();
    assertTrue(actualProps1.containsKey(ProducerConfig.BATCH_SIZE_CONFIG));
    assertEquals(
        actualProps1.get(ProducerConfig.BATCH_SIZE_CONFIG),
        ApacheKafkaProducerConfig.DEFAULT_KAFKA_BATCH_SIZE);
    assertTrue(actualProps1.containsKey(ProducerConfig.LINGER_MS_CONFIG));
    assertEquals(actualProps1.get(ProducerConfig.LINGER_MS_CONFIG), ApacheKafkaProducerConfig.DEFAULT_KAFKA_LINGER_MS);

    // should not override if already set
    veniceProperties.put(PubSubConstants.PUBSUB_PRODUCER_USE_HIGH_THROUGHPUT_DEFAULTS, "true");
    veniceProperties.put(ApacheKafkaProducerConfig.KAFKA_CONFIG_PREFIX + ProducerConfig.BATCH_SIZE_CONFIG, "55");
    veniceProperties.put(ApacheKafkaProducerConfig.KAFKA_CONFIG_PREFIX + ProducerConfig.LINGER_MS_CONFIG, "66");
    context = new PubSubProducerAdapterContext.Builder().setVeniceProperties(new VeniceProperties(veniceProperties))
        .setBrokerAddress(KAFKA_BROKER_ADDR)
        .setShouldValidateProducerConfigStrictly(false)
        .setProducerName(PRODUCER_NAME)
        .build();
    ApacheKafkaProducerConfig apacheKafkaProducerConfig2 = new ApacheKafkaProducerConfig(context);
    Properties actualProps2 = apacheKafkaProducerConfig2.getProducerProperties();
    assertTrue(actualProps2.containsKey(ProducerConfig.BATCH_SIZE_CONFIG));
    assertEquals(actualProps2.get(ProducerConfig.BATCH_SIZE_CONFIG), "55");
    assertTrue(actualProps2.containsKey(ProducerConfig.LINGER_MS_CONFIG));
    assertEquals(actualProps2.get(ProducerConfig.LINGER_MS_CONFIG), "66");
  }

  @Test
  public void testGetValidProducerProperties() {
    Properties allProps = new Properties();
    allProps.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "1000");
    allProps.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, "2000");
    // this is common config; there are no admin specific configs
    allProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    allProps.put("bogus.kafka.config", "bogusValue");

    Properties validProps = ApacheKafkaProducerConfig.getValidProducerProperties(allProps);
    assertEquals(validProps.size(), 2);
    assertEquals(validProps.get(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG), "localhost:9092");
    assertEquals(validProps.get(ProducerConfig.MAX_BLOCK_MS_CONFIG), "1000");
  }
}
