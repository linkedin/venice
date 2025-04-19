package com.linkedin.venice.pubsub.adapter.kafka;

import static com.linkedin.venice.pubsub.adapter.kafka.ApacheKafkaUtils.KAFKA_SSL_MANDATORY_CONFIGS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.pubsub.api.EmptyPubSubMessageHeaders;
import com.linkedin.venice.pubsub.api.PubSubMessageHeader;
import com.linkedin.venice.pubsub.api.PubSubMessageHeaders;
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
  public void testGenerateClientId() throws InterruptedException {
    // Case 1: Both clientName and brokerAddress are provided
    String clientId1 = ApacheKafkaUtils.generateClientId("consumerA", "broker-123");
    assertNotNull(clientId1, "Client ID should not be null");
    assertTrue(clientId1.startsWith("consumerA-broker-123-"), "Client ID format is incorrect");

    // Case 2: Only clientName is provided
    String clientId2 = ApacheKafkaUtils.generateClientId("consumerB", null);
    assertNotNull(clientId2, "Client ID should not be null");
    assertTrue(clientId2.startsWith("consumerB--"), "Client ID format is incorrect");

    // Case 3: Only brokerAddress is provided
    String clientId3 = ApacheKafkaUtils.generateClientId(null, "broker-456");
    assertNotNull(clientId3, "Client ID should not be null");
    assertTrue(clientId3.startsWith("kc-broker-456-"), "Client ID format is incorrect");

    // Case 4: Both parameters are null (defaults should be used)
    String clientId4 = ApacheKafkaUtils.generateClientId(null, null);
    assertNotNull(clientId4, "Client ID should not be null");
    assertTrue(clientId4.startsWith("kc--"), "Client ID format is incorrect");

    // Case 5: Ensure uniqueness between two generated IDs
    String clientId5 = ApacheKafkaUtils.generateClientId("consumerC", "broker-789");
    Thread.sleep(1); // Ensure timestamp difference
    String clientId6 = ApacheKafkaUtils.generateClientId("consumerC", "broker-789");

    assertNotEquals(clientId5, clientId6, "Generated Client IDs should be unique");
  }

  @Test
  public void testCopySaslConfiguration() {
    String SASL_JAAS_CONFIG =
        "org.apache.kafka.common.security.plain.PlainLoginModule required " + "username=\"foo\" password=\"bar\"\n";
    String SASL_MECHANISM = "PLAIN";
    Properties config = new Properties();
    config.put("kafka.sasl.jaas.config", SASL_JAAS_CONFIG);
    config.put("kafka.sasl.mechanism", SASL_MECHANISM);
    config.put("kafka.security.protocol", "SASL_SSL");
    KAFKA_SSL_MANDATORY_CONFIGS.forEach(configName -> config.put(configName, configName + "DefaultValue"));
    VeniceProperties veniceProperties = new VeniceProperties(config);
    Properties filteredConfig =
        ApacheKafkaUtils.getValidKafkaClientProperties(veniceProperties, ProducerConfig.configNames());
    assertEquals(filteredConfig.get("sasl.jaas.config"), SASL_JAAS_CONFIG);
    assertEquals(filteredConfig.get("sasl.mechanism"), SASL_MECHANISM);
    assertEquals(filteredConfig.get("security.protocol"), "SASL_SSL");
  }
}
