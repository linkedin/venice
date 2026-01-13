package com.linkedin.venice.utils;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.expectThrows;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.exceptions.UndefinedPropertyException;
import com.linkedin.venice.exceptions.VeniceException;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
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
    Map<String, String> props = new HashMap<>();
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
  public void testGetOrDefaultString() {
    Map<CharSequence, CharSequence> props = new HashMap<>();
    props.put("Foo", "bar");
    VeniceProperties veniceProperties = VeniceProperties.fromCharSequenceMap(props);

    assertEquals(veniceProperties.getOrDefault("Foo", "yuck"), "bar");
    assertEquals(veniceProperties.getOrDefault("Bar", "Foo"), "Foo");
  }

  @Test
  public void testClipAndFilterNamespaceSingleNamespace() {
    Map<CharSequence, CharSequence> props = new HashMap<>();
    props.put("kafka.key1", "value1");
    props.put("kafka.key2", "value2");
    props.put("database.host", "localhost");
    props.put("database.port", "5432");

    VeniceProperties veniceProperties = VeniceProperties.fromCharSequenceMap(props);

    VeniceProperties kafkaProps = veniceProperties.clipAndFilterNamespace("kafka.");
    Properties kafkaProperties = kafkaProps.toProperties();

    assertEquals(kafkaProperties.size(), 2);
    assertEquals(kafkaProperties.getProperty("key1"), "value1");
    assertEquals(kafkaProperties.getProperty("key2"), "value2");
  }

  @Test
  public void testClipAndFilterNamespaceNoMatchingProperties() {
    Map<String, String> props = new HashMap<>();
    props.put("app.config.path", "/usr/local/");
    props.put("logging.level", "DEBUG");

    VeniceProperties veniceProperties = new VeniceProperties(props);

    VeniceProperties kafkaProps = veniceProperties.clipAndFilterNamespace("kafka.");
    Properties kafkaProperties = kafkaProps.toProperties();

    assertEquals(kafkaProperties.size(), 0);
  }

  @Test
  public void testClipAndFilterNamespaceWithEmptyNamespacesSet() {
    Map<String, String> props = new HashMap<>();
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

    VeniceProperties veniceProperties = VeniceProperties.fromCharSequenceMap(props);

    VeniceProperties filteredProps = veniceProperties.clipAndFilterNamespace("kafka.");
    Properties resultProperties = filteredProps.toProperties();

    assertEquals(resultProperties.size(), 3);
    assertEquals(resultProperties.getProperty("key1"), "value1");
    assertEquals(resultProperties.getProperty("key2"), "value2");
    assertEquals(resultProperties.getProperty("key3"), "value3");
  }

  @Test
  public void testGetIntKeyedMap() {
    // Case: Valid comma-separated string with integer keys.
    Properties properties = new Properties();
    properties.put(
        ConfigKeys.PUBSUB_TYPE_ID_TO_POSITION_CLASS_NAME_MAP,
        "1:com.linkedin.venice.pubsub.kafka.ApacheKafkaOffsetPosition,2:com.linkedin.venice.pubsub.pulsar.ApachePulsarPosition");
    VeniceProperties veniceProperties = new VeniceProperties(properties);
    Int2ObjectMap<String> result =
        veniceProperties.getIntKeyedMap(ConfigKeys.PUBSUB_TYPE_ID_TO_POSITION_CLASS_NAME_MAP);
    assertEquals(result.size(), 2);
    assertEquals(result.get(1), "com.linkedin.venice.pubsub.kafka.ApacheKafkaOffsetPosition");
    assertEquals(result.get(2), "com.linkedin.venice.pubsub.pulsar.ApachePulsarPosition");

    // Case: An entry with a non-integer key should trigger a VeniceException.
    properties.put("bad.int.key", "abc:badValue,3:broker3.kafka.com:9094");
    VeniceProperties badVeniceProperties = new VeniceProperties(properties);
    expectThrows(VeniceException.class, () -> badVeniceProperties.getIntKeyedMap("bad.int.key"));

    // Case: Missing key should trigger UndefinedPropertyException.
    expectThrows(UndefinedPropertyException.class, () -> badVeniceProperties.getIntKeyedMap("non.existent.key"));
  }

  @Test
  public void testMapToString() {
    Map<String, String> map = new HashMap<>();
    map.put("key1", "value1");
    map.put("key2", "value2");
    map.put("key3", "value3");

    String result = VeniceProperties.mapToString(map);
    assertEquals(result, "key1:value1,key2:value2,key3:value3");

    // Test with empty map
    Map<String, String> emptyMap = new HashMap<>();
    String emptyResult = VeniceProperties.mapToString(emptyMap);
    assertEquals(emptyResult, "");
  }

  @Test
  public void testGetLongWithEmptyString() {
    // Test getLong with default value when property is empty string
    Properties properties = new Properties();
    properties.put("empty.long", "");
    properties.put("whitespace.long", "   ");
    properties.put("valid.long", "12345");
    VeniceProperties veniceProperties = new VeniceProperties(properties);

    // Empty string should return default value
    assertEquals(veniceProperties.getLong("empty.long", 999L), 999L);

    // Whitespace-only string should return default value
    assertEquals(veniceProperties.getLong("whitespace.long", 888L), 888L);

    // Valid value should be parsed
    assertEquals(veniceProperties.getLong("valid.long", 999L), 12345L);

    // Missing property should return default value
    assertEquals(veniceProperties.getLong("missing.long", 777L), 777L);
  }

  @Test
  public void testGetLongWithoutDefaultThrowsOnEmptyString() {
    // Test getLong without default value when property is empty string
    Properties properties = new Properties();
    properties.put("empty.long", "");
    properties.put("whitespace.long", "  ");
    VeniceProperties veniceProperties = new VeniceProperties(properties);

    // Empty string should throw VeniceException
    VeniceException ex1 = expectThrows(VeniceException.class, () -> veniceProperties.getLong("empty.long"));
    Assert.assertTrue(ex1.getMessage().contains("empty value"));

    // Whitespace-only string should throw VeniceException
    VeniceException ex2 = expectThrows(VeniceException.class, () -> veniceProperties.getLong("whitespace.long"));
    Assert.assertTrue(ex2.getMessage().contains("empty value"));

    // Missing property should throw UndefinedPropertyException
    expectThrows(UndefinedPropertyException.class, () -> veniceProperties.getLong("missing.long"));
  }

  @Test
  public void testGetIntWithEmptyString() {
    // Test getInt with default value when property is empty string
    Properties properties = new Properties();
    properties.put("empty.int", "");
    properties.put("whitespace.int", "   ");
    properties.put("valid.int", "42");
    VeniceProperties veniceProperties = new VeniceProperties(properties);

    // Empty string should return default value
    assertEquals(veniceProperties.getInt("empty.int", 100), 100);

    // Whitespace-only string should return default value
    assertEquals(veniceProperties.getInt("whitespace.int", 200), 200);

    // Valid value should be parsed
    assertEquals(veniceProperties.getInt("valid.int", 100), 42);

    // Missing property should return default value
    assertEquals(veniceProperties.getInt("missing.int", 300), 300);
  }

  @Test
  public void testGetIntWithoutDefaultThrowsOnEmptyString() {
    // Test getInt without default value when property is empty string
    Properties properties = new Properties();
    properties.put("empty.int", "");
    properties.put("whitespace.int", "  ");
    VeniceProperties veniceProperties = new VeniceProperties(properties);

    // Empty string should throw VeniceException
    VeniceException ex1 = expectThrows(VeniceException.class, () -> veniceProperties.getInt("empty.int"));
    Assert.assertTrue(ex1.getMessage().contains("empty value"));

    // Whitespace-only string should throw VeniceException
    VeniceException ex2 = expectThrows(VeniceException.class, () -> veniceProperties.getInt("whitespace.int"));
    Assert.assertTrue(ex2.getMessage().contains("empty value"));

    // Missing property should throw UndefinedPropertyException
    expectThrows(UndefinedPropertyException.class, () -> veniceProperties.getInt("missing.int"));
  }

  @Test
  public void testGetOptionalIntWithEmptyString() {
    // Test getOptionalInt when property is empty string
    Properties properties = new Properties();
    properties.put("empty.int", "");
    properties.put("whitespace.int", "   ");
    properties.put("valid.int", "123");
    VeniceProperties veniceProperties = new VeniceProperties(properties);

    // Empty string should return Optional.empty()
    Assert.assertFalse(veniceProperties.getOptionalInt("empty.int").isPresent());

    // Whitespace-only string should return Optional.empty()
    Assert.assertFalse(veniceProperties.getOptionalInt("whitespace.int").isPresent());

    // Valid value should be present
    Assert.assertTrue(veniceProperties.getOptionalInt("valid.int").isPresent());
    assertEquals(veniceProperties.getOptionalInt("valid.int").get(), Integer.valueOf(123));

    // Missing property should return Optional.empty()
    Assert.assertFalse(veniceProperties.getOptionalInt("missing.int").isPresent());
  }

  @Test
  public void testGetDoubleWithEmptyString() {
    // Test getDouble with default value when property is empty string
    Properties properties = new Properties();
    properties.put("empty.double", "");
    properties.put("whitespace.double", "   ");
    properties.put("valid.double", "3.15");
    VeniceProperties veniceProperties = new VeniceProperties(properties);

    // Empty string should return default value
    assertEquals(veniceProperties.getDouble("empty.double", 1.5), 1.5);

    // Whitespace-only string should return default value
    assertEquals(veniceProperties.getDouble("whitespace.double", 2.5), 2.5);

    // Valid value should be parsed
    assertEquals(veniceProperties.getDouble("valid.double", 1.5), 3.15);

    // Missing property should return default value
    assertEquals(veniceProperties.getDouble("missing.double", 4.5), 4.5);
  }

  @Test
  public void testGetDoubleWithoutDefaultThrowsOnEmptyString() {
    // Test getDouble without default value when property is empty string
    Properties properties = new Properties();
    properties.put("empty.double", "");
    properties.put("whitespace.double", "  ");
    VeniceProperties veniceProperties = new VeniceProperties(properties);

    // Empty string should throw VeniceException
    VeniceException ex1 = expectThrows(VeniceException.class, () -> veniceProperties.getDouble("empty.double"));
    Assert.assertTrue(ex1.getMessage().contains("empty value"));

    // Whitespace-only string should throw VeniceException
    VeniceException ex2 = expectThrows(VeniceException.class, () -> veniceProperties.getDouble("whitespace.double"));
    Assert.assertTrue(ex2.getMessage().contains("empty value"));

    // Missing property should throw UndefinedPropertyException
    expectThrows(UndefinedPropertyException.class, () -> veniceProperties.getDouble("missing.double"));
  }

  @Test
  public void testGetSizeInBytesWithEmptyString() {
    // Test getSizeInBytes with default value when property is empty string
    Properties properties = new Properties();
    properties.put("empty.size", "");
    properties.put("whitespace.size", "   ");
    properties.put("valid.size", "10MB");
    VeniceProperties veniceProperties = new VeniceProperties(properties);

    // Empty string should return default value
    assertEquals(veniceProperties.getSizeInBytes("empty.size", 1024L), 1024L);

    // Whitespace-only string should return default value
    assertEquals(veniceProperties.getSizeInBytes("whitespace.size", 2048L), 2048L);

    // Valid value should be parsed
    assertEquals(veniceProperties.getSizeInBytes("valid.size", 1024L), 10 * 1024 * 1024L);

    // Missing property should return default value
    assertEquals(veniceProperties.getSizeInBytes("missing.size", 4096L), 4096L);
  }

  @Test
  public void testGetSizeInBytesWithoutDefaultThrowsOnEmptyString() {
    // Test getSizeInBytes without default value when property is empty string
    Properties properties = new Properties();
    properties.put("empty.size", "");
    properties.put("whitespace.size", "  ");
    VeniceProperties veniceProperties = new VeniceProperties(properties);

    // Empty string should throw VeniceException
    VeniceException ex1 = expectThrows(VeniceException.class, () -> veniceProperties.getSizeInBytes("empty.size"));
    Assert.assertTrue(ex1.getMessage().contains("empty value"));

    // Whitespace-only string should throw VeniceException
    VeniceException ex2 = expectThrows(VeniceException.class, () -> veniceProperties.getSizeInBytes("whitespace.size"));
    Assert.assertTrue(ex2.getMessage().contains("empty value"));

    // Missing property should throw UndefinedPropertyException
    expectThrows(UndefinedPropertyException.class, () -> veniceProperties.getSizeInBytes("missing.size"));
  }
}
