package com.linkedin.venice.pubsub.adapter.kafka;

import static com.linkedin.venice.pubsub.adapter.kafka.ApacheKafkaUtils.KAFKA_SSL_MANDATORY_CONFIGS;
import static com.linkedin.venice.pubsub.adapter.kafka.producer.ApacheKafkaProducerConfig.KAFKA_PRODUCER_CONFIG_PREFIXES;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.pubsub.api.EmptyPubSubMessageHeaders;
import com.linkedin.venice.pubsub.api.PubSubMessageHeader;
import com.linkedin.venice.pubsub.api.PubSubMessageHeaders;
import com.linkedin.venice.pubsub.api.PubSubSecurityProtocol;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.function.Supplier;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.testng.annotations.Test;


public class ApacheKafkaUtilsTest {
  @Test
  public void testConvertToKafkaSpecificHeadersWhenPubsubMessageHeadersIsNullOrEmpty() {
    Supplier<PubSubMessageHeaders>[] suppliers =
        new Supplier[] { () -> null, () -> new PubSubMessageHeaders(), () -> EmptyPubSubMessageHeaders.SINGLETON };
    for (Supplier<PubSubMessageHeaders> supplier: suppliers) {
      RecordHeaders recordHeaders = ApacheKafkaUtils.convertToKafkaSpecificHeaders(supplier.get());
      assertNotNull(recordHeaders);
      assertEquals(recordHeaders.toArray().length, 0);
      assertThrows(() -> recordHeaders.add("foo", "bar".getBytes()));

      RecordHeaders recordHeaders2 = ApacheKafkaUtils.convertToKafkaSpecificHeaders(supplier.get());
      assertNotNull(recordHeaders2);
      assertEquals(recordHeaders2.toArray().length, 0);
      assertThrows(() -> recordHeaders2.add("foo", "bar".getBytes()));

      assertSame(recordHeaders, recordHeaders2);
    }
  }

  @Test
  public void testConvertToKafkaSpecificHeaders() {
    Map<String, String> headerMap = new LinkedHashMap<>();
    headerMap.put("key-0", "val-0");
    headerMap.put("key-1", "val-1");
    headerMap.put("key-2", "val-2");
    PubSubMessageHeaders pubsubMessageHeaders = new PubSubMessageHeaders();
    for (Map.Entry<String, String> entry: headerMap.entrySet()) {
      pubsubMessageHeaders.add(new PubSubMessageHeader(entry.getKey(), entry.getValue().getBytes()));
    }

    RecordHeaders recordHeaders = ApacheKafkaUtils.convertToKafkaSpecificHeaders(pubsubMessageHeaders);
    assertEquals(recordHeaders.toArray().length, pubsubMessageHeaders.toList().size());
    for (Header header: recordHeaders.toArray()) {
      assertTrue(headerMap.containsKey(header.key()));
      assertEquals(header.value(), headerMap.get(header.key()).getBytes());
    }
  }

  @Test
  public void testCopySaslConfiguration() {
    String SASL_JAAS_CONFIG =
        "org.apache.kafka.common.security.plain.PlainLoginModule required " + "username=\"foo\" password=\"bar\"\n";
    String SASL_MECHANISM = "PLAIN";
    Properties config = new Properties();
    config.put("kafka.sasl.jaas.config", SASL_JAAS_CONFIG);
    config.put("kafka.sasl.mechanism", SASL_MECHANISM);
    KAFKA_SSL_MANDATORY_CONFIGS.forEach(configName -> config.put(configName, configName + "DefaultValue"));
    VeniceProperties veniceProperties = new VeniceProperties(config);
    Properties filteredConfig = ApacheKafkaUtils.getValidKafkaClientProperties(
        veniceProperties,
        PubSubSecurityProtocol.SASL_SSL,
        ProducerConfig.configNames(),
        KAFKA_PRODUCER_CONFIG_PREFIXES);
    assertEquals(filteredConfig.get("sasl.jaas.config"), SASL_JAAS_CONFIG);
    assertEquals(filteredConfig.get("sasl.mechanism"), SASL_MECHANISM);
    assertEquals(filteredConfig.get("security.protocol"), "SASL_SSL");
  }

  @Test
  public void testIsKafkaProtocolValid() {
    assertTrue(ApacheKafkaUtils.isKafkaProtocolValid("SSL"));
    assertTrue(ApacheKafkaUtils.isKafkaProtocolValid("PLAINTEXT"));
    assertTrue(ApacheKafkaUtils.isKafkaProtocolValid("SASL_SSL"));
    assertTrue(ApacheKafkaUtils.isKafkaProtocolValid("SASL_PLAINTEXT"));
  }
}
