package com.linkedin.venice.utils;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.linkedin.venice.exceptions.UndefinedPropertyException;
import java.util.Properties;
import org.testng.annotations.Test;


/**
 * Created by athirupa on 4/21/16.
 */
public class PropertyBuilderTest {
  @Test
  public void testStringPutAPI() {
    PropertyBuilder builder = new PropertyBuilder();
    String key = "string.key";
    String originalValue = "value";
    String anotherValue = "value2";

    // Non Existent Get should throw Exception
    try {
      builder.build().getString(key);
      fail("Expected an undefined property exception for Key " + key);
    } catch (UndefinedPropertyException ex) {
      // expected
    }

    // Non Existent with default should return default value.
    String retrievedValue = builder.build().getString(key, anotherValue);
    assertEquals(anotherValue, retrievedValue, "Default value is expected");

    // Put one value and verify it can be retrieved.
    builder.put(key, originalValue);
    assertEquals(originalValue, builder.build().getString(key), "return value is different");

    // Update and make sure the value is updated.
    builder.put(key, anotherValue);
    assertEquals(anotherValue, builder.build().getString(key), "return value is different");

    // Call the override with default and the existing value should be different
    assertEquals(anotherValue, builder.build().getString(key, "randomValue"), "return value is different");
  }

  @Test
  public void testIntegerPutAPI() {
    PropertyBuilder builder = new PropertyBuilder();
    String key = "integer.key";
    int originalValue = Integer.MIN_VALUE;
    int anotherValue = Integer.MAX_VALUE;

    // Non Existent Get should throw Exception
    try {
      builder.build().getInt(key);
      fail("Expected an undefined property exception for Key " + key);
    } catch (UndefinedPropertyException ex) {
      // expected
    }

    // Non Existent with default should return default value.
    Integer retrievedValue = builder.build().getInt(key, 100);
    assertEquals(100, retrievedValue.intValue(), "Default value is expected");

    // Put one value and verify it can be retrieved.
    builder.put(key, originalValue);
    assertEquals(originalValue, builder.build().getInt(key), "return value is different");

    // Update and make sure the value is updated.
    builder.put(key, anotherValue);
    assertEquals(anotherValue, builder.build().getInt(key), "return value is different");

    // Call the override with default and the existing value should be different
    assertEquals(anotherValue, builder.build().getInt(key, 100), "return value is different");
  }

  @Test
  public void testLongPutAPI() {
    PropertyBuilder builder = new PropertyBuilder();
    String key = "long.key";
    long originalValue = Long.MIN_VALUE;
    long anotherValue = Long.MAX_VALUE;

    // Non Existent Get should throw Exception
    try {
      builder.build().getLong(key);
      fail("Expected an undefined property exception for Key " + key);
    } catch (UndefinedPropertyException ex) {
      // expected
    }

    // Non Existent with default should return default value.
    Long retrievedValue = builder.build().getLong(key, 100);
    assertEquals(100, retrievedValue.longValue(), "Default value is expected");

    // Put one value and verify it can be retrieved.
    builder.put(key, originalValue);
    assertEquals(originalValue, builder.build().getLong(key), "return value is different");

    // Update and make sure the value is updated.
    builder.put(key, anotherValue);
    assertEquals(anotherValue, builder.build().getLong(key), "return value is different");

    // Call the override with default and the existing value should be different
    assertEquals(anotherValue, builder.build().getLong(key, 100), "return value is different");
  }

  @Test
  public void testBooleanPutAPI() {
    PropertyBuilder builder = new PropertyBuilder();
    String key = "boolean.key";
    boolean originalValue = true;
    boolean anotherValue = false;

    // Non Existent Get should throw Exception
    try {
      builder.build().getBoolean(key);
      fail("Expected an undefined property exception for Key " + key);
    } catch (UndefinedPropertyException ex) {
      // expected
    }

    // Non Existent with default should return default value.
    boolean retrievedValue = builder.build().getBoolean(key, true);
    assertTrue(retrievedValue, "Default value is expected");

    // Put one value and verify it can be retrieved.
    builder.put(key, originalValue);
    assertEquals(originalValue, builder.build().getBoolean(key), "return value is different");

    // Update and make sure the value is updated.
    builder.put(key, anotherValue);
    assertEquals(anotherValue, builder.build().getBoolean(key), "return value is different");

    // Call the override with default and the existing value should be different
    assertFalse(builder.build().getBoolean(key, true), "return value is different");
  }

  @Test
  public void testBytesRetrieval() {
    PropertyBuilder builder = new PropertyBuilder();
    String key = "bytes.key";

    // Non Existent with default should return default value.
    try {
      builder.build().getSizeInBytes(key);
      fail("Expected an undefined property exception for Key " + key);
    } catch (UndefinedPropertyException ex) {
      // expected
    }

    long defaultValue = Long.MAX_VALUE;
    long retrievedValue = builder.build().getSizeInBytes(key, defaultValue);
    assertEquals(defaultValue, retrievedValue, "Default value is expected");

    // Put one value and verify it can be retrieved.
    builder.put(key, "100Kb");
    assertEquals(100 * 1024, builder.build().getSizeInBytes(key), "return value is different");

    // Update and make sure the value is updated.
    long hundredMegs = 100 * 1024 * 1024;
    builder.put(key, "100m");
    assertEquals(hundredMegs, builder.build().getSizeInBytes(key), "return value is different");
    builder.put(key, hundredMegs);
    assertEquals(hundredMegs, builder.build().getSizeInBytes(key), "return value is different");

    // Call the override with default and the existing value should be different
    assertEquals(hundredMegs, builder.build().getSizeInBytes(key, 1), "return value is different");
  }

  @Test
  public void testExtractProperties() {
    Properties props = new Properties();
    props.setProperty("cluster.name", "test-cluster");
    props.setProperty("kafka.brokers", "localhost");
    props.setProperty("helix.enabled", "false");
    props.setProperty("kafka.broker.port", "9092");
    props.setProperty("default.persistence.type", "IN_MEMORY");
    props.setProperty("kafka.consumer.fetch.buffer.size", "65536");
    props.setProperty("zookeeper.address", "localhost:2181");

    VeniceProperties originalProps = new VeniceProperties(props);

    Properties otherProps = new Properties();
    otherProps.setProperty("brokers", "localhost");
    otherProps.setProperty("broker.port", "9092");
    otherProps.setProperty("consumer.fetch.buffer.size", "65536");

    VeniceProperties expectedProps = new VeniceProperties(otherProps);
    VeniceProperties computedProps = originalProps.clipAndFilterNamespace("kafka");
    VeniceProperties otherComputedProps = originalProps.clipAndFilterNamespace("kafka.");

    assertEquals(expectedProps, computedProps, "Stripped props does not match");
    assertEquals(otherComputedProps, computedProps, "Stripped props does not match");
    assertNotEquals(originalProps, computedProps, " two properties should not match");
  }

  @Test
  public void testStoreProperties() {
    PropertyBuilder builder = new PropertyBuilder();
    builder.put("base.only", 123)
        .put("base.only2", "large")
        // Pick only this store value
        .put("base.overridden", "12")
        .put("store-foobar.base.overridden", "34")
        .put("store-ignore.base.overridden", "56")
        // If other stores are overriden, it should be ignored and should get base.
        .put("base.value", "base")
        .put("store-ignore.base.value", "dontGetThis")
        // Pick properties that are not present in base, but only specific store.
        .put("store-foobar.store.only", 567);

    VeniceProperties props = builder.build();

    VeniceProperties computedStoreProps = props.getStoreProperties("foobar");

    PropertyBuilder storePropsBuilder = new PropertyBuilder();
    storePropsBuilder.put("base.only", 123)
        .put("base.only2", "large")
        .put("base.overridden", "34")
        .put("base.value", "base")
        .put("store.only", 567);

    VeniceProperties storeProps = storePropsBuilder.build();

    assertEquals(storeProps, computedStoreProps, "Store Properties are not the expected");

  }
}
