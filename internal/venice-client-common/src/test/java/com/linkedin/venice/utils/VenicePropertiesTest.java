package com.linkedin.venice.utils;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

import com.linkedin.venice.exceptions.VeniceException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.testng.Assert;
import org.testng.annotations.Test;


public class VenicePropertiesTest {
  @Test
  public void testConvertSizeFromLiteral() {
    Assert.assertEquals(VeniceProperties.convertSizeFromLiteral("512"), 512l);
    Assert.assertEquals(VeniceProperties.convertSizeFromLiteral("1KB"), 1024l);
    Assert.assertEquals(VeniceProperties.convertSizeFromLiteral("1k"), 1024l);
    Assert.assertEquals(VeniceProperties.convertSizeFromLiteral("1MB"), 1024 * 1024l);
    Assert.assertEquals(VeniceProperties.convertSizeFromLiteral("1m"), 1024 * 1024l);
    Assert.assertEquals(VeniceProperties.convertSizeFromLiteral("1GB"), 1024 * 1024 * 1024l);
    Assert.assertEquals(VeniceProperties.convertSizeFromLiteral("1g"), 1024 * 1024 * 1024l);
  }

  @Test
  public void testGetMapWhenMapIsStringEncoded() {
    VeniceProperties veniceProperties =
        new PropertyBuilder().put("region.to.pubsub.broker.map", "prod:https://prod-broker:1234,dev:dev-broker:9876")
            .build();
    Map<String, String> map = veniceProperties.getMap("region.to.pubsub.broker.map");
    assertEquals(map.size(), 2);
    assertEquals(map.get("prod"), "https://prod-broker:1234");
    assertEquals(map.get("dev"), "dev-broker:9876");

    // Map should be immutable
    assertThrows(() -> map.put("foo", "bar"));

    // Invalid encoding
    VeniceProperties invalidVeniceProperties =
        new PropertyBuilder().put("region.to.pubsub.broker.map", "prod:https://prod-broker:1234,dev;dev-broker")
            .build();
    assertThrows(VeniceException.class, () -> invalidVeniceProperties.getMap("region.to.pubsub.broker.map"));
  }

  @Test
  public void testClipAndFilterNamespaceTwoNamespaces() {
    Map<CharSequence, CharSequence> props = new HashMap<>();
    props.put("kafka.key1", "value1");
    props.put("pubsub.kafka.key1", "value1");
    props.put("kafka.key2", "value2");
    props.put("kafka.key3.key30", "value330");
    props.put("pubsub.kafka.key3.key30", "value330");
    props.put("pubsub.kafka.key3.key31", "value331");
    props.put("bogus.kafka.key3.key32", "value332");
    VeniceProperties veniceProperties = new VeniceProperties(props);

    VeniceProperties kafkaPrefixProps = veniceProperties.clipAndFilterNamespace("kafka.");
    Properties kafkaPrefixProperties = kafkaPrefixProps.toProperties();
    assertEquals(kafkaPrefixProperties.size(), 3);
    assertEquals(kafkaPrefixProperties.getProperty("key1"), "value1");
    assertEquals(kafkaPrefixProperties.getProperty("key2"), "value2");
    assertEquals(kafkaPrefixProperties.getProperty("key3.key30"), "value330");

    VeniceProperties pubsubKafkaPrefixProps = veniceProperties.clipAndFilterNamespace("pubsub.kafka.");
    Properties pubsubKafkaPrefixProperties = pubsubKafkaPrefixProps.toProperties();
    assertEquals(pubsubKafkaPrefixProperties.size(), 3);
    assertEquals(pubsubKafkaPrefixProperties.getProperty("key1"), "value1");
    assertEquals(pubsubKafkaPrefixProperties.getProperty("key3.key30"), "value330");
    assertEquals(pubsubKafkaPrefixProperties.getProperty("key3.key31"), "value331");

    // Test both prefixes
    Set<String> prefixes = new HashSet<>(Arrays.asList("kafka.", "pubsub.kafka."));
    VeniceProperties bothPrefixProps = veniceProperties.clipAndFilterNamespace(prefixes);
    Properties bothPrefixProperties = bothPrefixProps.toProperties();
    assertEquals(bothPrefixProperties.size(), 4);
    assertEquals(bothPrefixProperties.getProperty("key1"), "value1");
    assertEquals(bothPrefixProperties.getProperty("key2"), "value2");
    assertEquals(bothPrefixProperties.getProperty("key3.key30"), "value330");
    assertEquals(bothPrefixProperties.getProperty("key3.key31"), "value331");
  }

  @Test
  public void testClipAndFilterNamespaceSingleNamespace() {
    Map<CharSequence, CharSequence> props = new HashMap<>();
    props.put("kafka.key1", "value1");
    props.put("kafka.key2", "value2");
    props.put("database.host", "localhost");
    props.put("database.port", "5432");

    VeniceProperties veniceProperties = new VeniceProperties(props);

    VeniceProperties kafkaProps = veniceProperties.clipAndFilterNamespace("kafka.");
    Properties kafkaProperties = kafkaProps.toProperties();

    assertEquals(kafkaProperties.size(), 2);
    assertEquals(kafkaProperties.getProperty("key1"), "value1");
    assertEquals(kafkaProperties.getProperty("key2"), "value2");
  }

  @Test
  public void testClipAndFilterNamespaceNoMatchingProperties() {
    Map<CharSequence, CharSequence> props = new HashMap<>();
    props.put("app.config.path", "/usr/local/");
    props.put("logging.level", "DEBUG");

    VeniceProperties veniceProperties = new VeniceProperties(props);

    VeniceProperties kafkaProps = veniceProperties.clipAndFilterNamespace("kafka.");
    Properties kafkaProperties = kafkaProps.toProperties();

    assertEquals(kafkaProperties.size(), 0);
  }

  @Test
  public void testClipAndFilterNamespaceWithEmptyNamespacesSet() {
    Map<CharSequence, CharSequence> props = new HashMap<>();
    props.put("kafka.key1", "value1");
    props.put("database.host", "localhost");

    VeniceProperties veniceProperties = new VeniceProperties(props);

    VeniceProperties emptyProps = veniceProperties.clipAndFilterNamespace(Collections.emptySet());
    Properties resultProperties = emptyProps.toProperties();

    assertEquals(resultProperties.size(), 0);
  }

  @Test
  public void testClipAndFilterNamespaceAllPropertiesFiltered() {
    Map<CharSequence, CharSequence> props = new HashMap<>();
    props.put("kafka.key1", "value1");
    props.put("kafka.key2", "value2");
    props.put("kafka.key3", "value3");

    VeniceProperties veniceProperties = new VeniceProperties(props);

    VeniceProperties filteredProps = veniceProperties.clipAndFilterNamespace("kafka.");
    Properties resultProperties = filteredProps.toProperties();

    assertEquals(resultProperties.size(), 3);
    assertEquals(resultProperties.getProperty("key1"), "value1");
    assertEquals(resultProperties.getProperty("key2"), "value2");
    assertEquals(resultProperties.getProperty("key3"), "value3");
  }
}
