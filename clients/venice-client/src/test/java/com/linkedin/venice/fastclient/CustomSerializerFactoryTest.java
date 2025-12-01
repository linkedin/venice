package com.linkedin.venice.fastclient;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.r2.transport.common.Client;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.serializer.RecordSerializer;
import java.nio.ByteBuffer;
import java.util.Optional;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.testng.annotations.Test;


/**
 * Tests to verify that custom SerializerFactory and DeserializerFactory
 * are properly integrated into the fast client.
 */
public class CustomSerializerFactoryTest {
  private static final String STORE_NAME = "test_store";
  private static final String KEY_SCHEMA_STR = "{\"type\":\"string\"}";
  private static final String VALUE_SCHEMA_STR =
      "{\"type\":\"record\",\"name\":\"TestValue\",\"fields\":[{\"name\":\"field1\",\"type\":\"string\"}]}";

  private ClientConfig.ClientConfigBuilder getClientConfigWithMinimumRequiredInputs() {
    return new ClientConfig.ClientConfigBuilder().setStoreName(STORE_NAME)
        .setR2Client(mock(Client.class))
        .setD2Client(mock(D2Client.class))
        .setClusterDiscoveryD2Service("test_service");
  }

  @Test
  public void testCustomKeySerializerFactoryIsUsed() {
    Schema keySchema = new Schema.Parser().parse(KEY_SCHEMA_STR);

    // Create a mock custom key serializer
    RecordSerializer customKeySerializer = mock(RecordSerializer.class);
    byte[] expectedSerializedKey = "custom_serialized_key".getBytes();
    when(customKeySerializer.serialize("test_key")).thenReturn(expectedSerializedKey);

    // Create a custom serializer factory
    SerializerFactory customSerializerFactory = mock(SerializerFactory.class);
    when(customSerializerFactory.createSerializer(any(Schema.class))).thenReturn(customKeySerializer);

    // Build client config with custom key serializer factory
    ClientConfig.ClientConfigBuilder configBuilder = getClientConfigWithMinimumRequiredInputs();
    configBuilder.setKeySerializerFactory(customSerializerFactory);

    ClientConfig clientConfig = configBuilder.build();

    // Verify the factory is present in the config
    assertTrue(clientConfig.getKeySerializerFactory().isPresent());
    assertEquals(clientConfig.getKeySerializerFactory().get(), customSerializerFactory);

    // Verify the factory creates the custom serializer
    Optional<SerializerFactory> factoryOptional = clientConfig.getKeySerializerFactory();
    assertTrue(factoryOptional.isPresent());
    RecordSerializer serializer = factoryOptional.get().createSerializer(keySchema);
    assertEquals(serializer, customKeySerializer);

    // Verify the custom serializer is used
    byte[] serializedKey = serializer.serialize("test_key");
    assertNotNull(serializedKey);
    assertEquals(serializedKey, expectedSerializedKey);
    verify(customKeySerializer, times(1)).serialize("test_key");
  }

  @Test
  public void testCustomValueDeserializerFactoryIsUsed() {
    Schema valueSchema = new Schema.Parser().parse(VALUE_SCHEMA_STR);

    // Create a mock custom value deserializer
    RecordDeserializer customValueDeserializer = mock(RecordDeserializer.class);
    GenericRecord expectedDeserializedValue = new GenericData.Record(valueSchema);
    expectedDeserializedValue.put("field1", "custom_value");

    ByteBuffer serializedValue = ByteBuffer.wrap("serialized_value".getBytes());
    when(customValueDeserializer.deserialize(serializedValue)).thenReturn(expectedDeserializedValue);

    // Create a custom deserializer factory
    DeserializerFactory customDeserializerFactory = mock(DeserializerFactory.class);
    when(customDeserializerFactory.createDeserializer(any(Schema.class))).thenReturn(customValueDeserializer);

    // Build client config with custom value deserializer factory
    ClientConfig.ClientConfigBuilder configBuilder = getClientConfigWithMinimumRequiredInputs();
    configBuilder.setValueDeserializerFactory(customDeserializerFactory);

    ClientConfig clientConfig = configBuilder.build();

    // Verify the factory is present in the config
    assertTrue(clientConfig.getValueDeserializerFactory().isPresent());
    assertEquals(clientConfig.getValueDeserializerFactory().get(), customDeserializerFactory);

    // Verify the factory creates the custom deserializer
    Optional<DeserializerFactory> factoryOptional = clientConfig.getValueDeserializerFactory();
    assertTrue(factoryOptional.isPresent());
    RecordDeserializer deserializer = factoryOptional.get().createDeserializer(valueSchema);
    assertEquals(deserializer, customValueDeserializer);

    // Verify the custom deserializer is used
    Object deserializedValue = deserializer.deserialize(serializedValue);
    assertNotNull(deserializedValue);
    assertEquals(deserializedValue, expectedDeserializedValue);
    verify(customValueDeserializer, times(1)).deserialize(serializedValue);
  }

  @Test
  public void testBothCustomFactoriesCanBeUsedTogether() {
    Schema keySchema = new Schema.Parser().parse(KEY_SCHEMA_STR);
    Schema valueSchema = new Schema.Parser().parse(VALUE_SCHEMA_STR);

    // Create mock custom serializer and deserializer
    RecordSerializer customKeySerializer = mock(RecordSerializer.class);
    RecordDeserializer customValueDeserializer = mock(RecordDeserializer.class);

    // Create custom factories
    SerializerFactory customSerializerFactory = mock(SerializerFactory.class);
    when(customSerializerFactory.createSerializer(any(Schema.class))).thenReturn(customKeySerializer);

    DeserializerFactory customDeserializerFactory = mock(DeserializerFactory.class);
    when(customDeserializerFactory.createDeserializer(any(Schema.class))).thenReturn(customValueDeserializer);

    // Build client config with both custom factories
    ClientConfig.ClientConfigBuilder configBuilder = getClientConfigWithMinimumRequiredInputs();
    configBuilder.setKeySerializerFactory(customSerializerFactory)
        .setValueDeserializerFactory(customDeserializerFactory);

    ClientConfig clientConfig = configBuilder.build();

    // Verify both factories are present
    assertTrue(clientConfig.getKeySerializerFactory().isPresent());
    assertTrue(clientConfig.getValueDeserializerFactory().isPresent());

    // Verify both factories work
    Optional<SerializerFactory> serializerFactoryOptional = clientConfig.getKeySerializerFactory();
    assertTrue(serializerFactoryOptional.isPresent());
    RecordSerializer serializer = serializerFactoryOptional.get().createSerializer(keySchema);
    assertEquals(serializer, customKeySerializer);

    Optional<DeserializerFactory> deserializerFactoryOptional = clientConfig.getValueDeserializerFactory();
    assertTrue(deserializerFactoryOptional.isPresent());
    RecordDeserializer deserializer = deserializerFactoryOptional.get().createDeserializer(valueSchema);
    assertEquals(deserializer, customValueDeserializer);
  }

  @Test
  public void testDefaultSerializersUsedWhenNoCustomFactories() {
    // Build client config without custom factories
    ClientConfig.ClientConfigBuilder configBuilder = getClientConfigWithMinimumRequiredInputs();

    ClientConfig clientConfig = configBuilder.build();

    // Verify no custom factories are set
    assertFalse(clientConfig.getKeySerializerFactory().isPresent());
    assertFalse(clientConfig.getValueDeserializerFactory().isPresent());
  }
}
