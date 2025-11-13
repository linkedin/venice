package com.linkedin.venice.fastclient;

import static org.testng.Assert.*;

import com.linkedin.r2.transport.common.Client;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.fastclient.factory.ClientFactory;
import com.linkedin.venice.fastclient.meta.StoreMetadataFetchMode;
import com.linkedin.venice.fastclient.utils.AbstractClientEndToEndSetup;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.serializer.VeniceSerializationException;
import com.linkedin.venice.stats.VeniceMetricsRepository;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.specific.SpecificRecord;
import org.testng.annotations.Test;


/**
 * End-to-end integration test for protobuf-like custom serialization/deserialization functionality
 * in the Venice Fast Client. This test validates that custom serializer and deserializer factories
 * work correctly through the entire stack.
 */
public class ProtoStoreClientEndToEndTest extends AbstractClientEndToEndSetup {
  /**
   * Simple POJO to simulate a protobuf message for testing.
   * In a real scenario, this would be a generated protobuf class.
   * This class represents the protobuf equivalent of the Avro record stored in Venice,
   * which only has an int_field.
   */
  public static class TestProtoMessage {
    private final int intField;

    public TestProtoMessage(int intField) {
      this.intField = intField;
    }

    public int getIntField() {
      return intField;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      TestProtoMessage that = (TestProtoMessage) o;
      return intField == that.intField;
    }

    @Override
    public int hashCode() {
      return Objects.hash(intField);
    }

    @Override
    public String toString() {
      return "TestProtoMessage{intField=" + intField + "}";
    }
  }

  /**
   * Custom serializer that converts TestProtoMessage to Avro GenericRecord,
   * simulating what ProtoToAvroRecordSerializer would do.
   */
  public static class ProtoToAvroSerializer implements RecordSerializer<TestProtoMessage> {
    private final Schema avroSchema;

    public ProtoToAvroSerializer(Schema avroSchema) {
      this.avroSchema = avroSchema;
    }

    @Override
    public byte[] serialize(TestProtoMessage protoMessage) throws VeniceSerializationException {
      if (protoMessage == null) {
        return null;
      }

      try {
        // Convert proto message to Avro GenericRecord
        GenericRecord avroRecord = new GenericData.Record(avroSchema);
        avroRecord.put(VALUE_FIELD_NAME, protoMessage.getIntField());

        // Use standard Avro serializer to convert to bytes
        com.linkedin.venice.serializer.AvroSerializer<GenericRecord> avroSerializer =
            new com.linkedin.venice.serializer.AvroSerializer<>(avroSchema);
        return avroSerializer.serialize(avroRecord);
      } catch (Exception e) {
        throw new VeniceSerializationException("Failed to serialize TestProtoMessage", e);
      }
    }

    @Override
    public byte[] serializeObjects(Iterable<TestProtoMessage> objects) throws VeniceException {
      return new byte[0];
    }

    @Override
    public byte[] serializeObjects(Iterable<TestProtoMessage> objects, ByteBuffer prefix) throws VeniceException {
      return new byte[0];
    }
  }

  /**
   * Custom deserializer that converts Avro bytes to TestProtoMessage,
   * simulating what RecordDeserializerToProto would do.
   */
  public static class AvroToProtoDeserializer implements RecordDeserializer<TestProtoMessage> {
    private final com.linkedin.venice.serializer.AvroGenericDeserializer<GenericRecord> avroDeserializer;

    public AvroToProtoDeserializer(Schema writerSchema, Schema readerSchema) {
      this.avroDeserializer = new com.linkedin.venice.serializer.AvroGenericDeserializer<>(writerSchema, readerSchema);
    }

    @Override
    public TestProtoMessage deserialize(byte[] bytes) throws VeniceSerializationException {
      if (bytes == null) {
        return null;
      }

      try {
        GenericRecord avroRecord = avroDeserializer.deserialize(bytes);
        if (avroRecord == null) {
          return null;
        }

        // Convert Avro GenericRecord to proto message
        int intField = (Integer) avroRecord.get(VALUE_FIELD_NAME);
        return new TestProtoMessage(intField);
      } catch (Exception e) {
        throw new VeniceSerializationException("Failed to deserialize to TestProtoMessage", e);
      }
    }

    @Override
    public TestProtoMessage deserialize(ByteBuffer byteBuffer) throws VeniceSerializationException {
      return deserialize(avroDeserializer.deserialize(byteBuffer));
    }

    private TestProtoMessage deserialize(GenericRecord avroRecord) {
      if (avroRecord == null) {
        return null;
      }
      int intField = (Integer) avroRecord.get(VALUE_FIELD_NAME);
      return new TestProtoMessage(intField);
    }

    @Override
    public TestProtoMessage deserialize(TestProtoMessage reuse, ByteBuffer byteBuffer, BinaryDecoder reusedDecoder)
        throws VeniceSerializationException {
      GenericRecord avroRecord = avroDeserializer.deserialize(null, byteBuffer, reusedDecoder);
      return deserialize(avroRecord);
    }

    @Override
    public TestProtoMessage deserialize(TestProtoMessage reuse, byte[] bytes) throws VeniceSerializationException {
      return deserialize(bytes);
    }

    @Override
    public TestProtoMessage deserialize(BinaryDecoder binaryDecoder) throws VeniceSerializationException {
      GenericRecord avroRecord = avroDeserializer.deserialize(binaryDecoder);
      return deserialize(avroRecord);
    }

    @Override
    public TestProtoMessage deserialize(TestProtoMessage reuse, BinaryDecoder binaryDecoder)
        throws VeniceSerializationException {
      return deserialize(binaryDecoder);
    }

    @Override
    public TestProtoMessage deserialize(TestProtoMessage reuse, InputStream in, BinaryDecoder reusedDecoder)
        throws VeniceSerializationException {
      GenericRecord avroRecord = avroDeserializer.deserialize(null, in, reusedDecoder);
      return deserialize(avroRecord);
    }

    @Override
    public List<TestProtoMessage> deserializeObjects(byte[] bytes) throws VeniceSerializationException {
      throw new UnsupportedOperationException("Batch deserialization not implemented for test");
    }

    @Override
    public List<TestProtoMessage> deserializeObjects(BinaryDecoder binaryDecoder) throws VeniceSerializationException {
      throw new UnsupportedOperationException("Batch deserialization not implemented for test");
    }
  }

  /**
   * Test that fast client can use custom serializer and deserializer factories for protobuf-like objects.
   * In this test, keys remain as strings (standard Venice keys), but values are custom protobuf objects.
   */
  @Test(timeOut = TIME_OUT)
  public void testFastClientWithCustomProtoSerDe() throws Exception {
    // Create custom deserializer factory for protobuf values
    // Keys remain as standard strings, only values use protobuf
    DeserializerFactory<TestProtoMessage> valueDeserializerFactory =
        (writerSchema, readerSchema) -> new AvroToProtoDeserializer(writerSchema, readerSchema);

    // Build client config with custom value deserializer
    Client r2Client = getR2Client();
    ClientConfig.ClientConfigBuilder<String, TestProtoMessage, SpecificRecord> clientConfigBuilder =
        new ClientConfig.ClientConfigBuilder<String, TestProtoMessage, SpecificRecord>().setStoreName(storeName)
            .setR2Client(r2Client)
            .setValueDeserializerFactory(valueDeserializerFactory);

    VeniceMetricsRepository metricsRepository = createVeniceMetricsRepository(false);

    AvroGenericStoreClient<String, TestProtoMessage> protoFastClient = null;

    try {
      // Create fast client with proto value deserializer
      setupStoreMetadata(clientConfigBuilder, StoreMetadataFetchMode.SERVER_BASED_METADATA);
      clientConfigBuilder.setMetricsRepository(metricsRepository);
      ClientConfig clientConfig = clientConfigBuilder.build();

      protoFastClient = ClientFactory.getAndStartGenericStoreClient(clientConfig);

      // Test single get with proto deserialization
      for (int i = 0; i < 10; ++i) {
        String key = keyPrefix + i;
        TestProtoMessage value = protoFastClient.get(key).get();

        assertNotNull(value, "Value should not be null for key " + i);
        assertEquals(value.getIntField(), i, "Value int field should match");
      }

      // Test batch get with proto deserialization
      Set<String> keys = new HashSet<>();
      for (int i = 0; i < 10; ++i) {
        keys.add(keyPrefix + i);
      }

      Map<String, TestProtoMessage> resultMap = protoFastClient.batchGet(keys).get();
      assertEquals(resultMap.size(), 10, "Should get all 10 values");

      for (int i = 0; i < 10; ++i) {
        String key = keyPrefix + i;
        TestProtoMessage value = resultMap.get(key);

        assertNotNull(value, "Value should not be null for key " + key);
        assertEquals(value.getIntField(), i, "Value int field should match");
      }

      // Test non-existent key
      String nonExistentKey = "nonExistentKey";
      TestProtoMessage result = protoFastClient.get(nonExistentKey).get();
      assertNull(result, "Should return null for non-existent key");

    } finally {
      if (protoFastClient != null) {
        protoFastClient.close();
      }
    }
  }

  /**
   * Test that custom serializer factory is properly invoked during key serialization.
   */
  @Test(timeOut = TIME_OUT)
  public void testCustomSerializerFactoryInvocation() throws Exception {
    final boolean[] serializerCreated = new boolean[] { false };

    // Create a factory that tracks whether it was called
    SerializerFactory<String> trackingSerializerFactory = (keySchema) -> {
      serializerCreated[0] = true;
      return new com.linkedin.venice.serializer.AvroSerializer<>(keySchema);
    };

    Client r2Client = getR2Client();
    ClientConfig.ClientConfigBuilder<String, GenericRecord, SpecificRecord> clientConfigBuilder =
        new ClientConfig.ClientConfigBuilder<String, GenericRecord, SpecificRecord>().setStoreName(storeName)
            .setR2Client(r2Client)
            .setKeySerializerFactory(trackingSerializerFactory);

    VeniceMetricsRepository metricsRepository = createVeniceMetricsRepository(false);

    AvroGenericStoreClient<String, GenericRecord> fastClient = null;

    try {
      setupStoreMetadata(clientConfigBuilder, StoreMetadataFetchMode.SERVER_BASED_METADATA);
      clientConfigBuilder.setMetricsRepository(metricsRepository);
      ClientConfig clientConfig = clientConfigBuilder.build();

      fastClient = ClientFactory.getAndStartGenericStoreClient(clientConfig);

      // Perform a get operation which should trigger serializer creation
      fastClient.get(keyPrefix + "0").get();

      // Verify the serializer factory was called
      assertTrue(serializerCreated[0], "Serializer factory should have been invoked");

    } finally {
      if (fastClient != null) {
        fastClient.close();
      }
    }
  }

  /**
   * Test that custom deserializer factory is properly invoked during value deserialization.
   */
  @Test(timeOut = TIME_OUT)
  public void testCustomDeserializerFactoryInvocation() throws Exception {
    final boolean[] deserializerCreated = new boolean[] { false };

    // Create a factory that tracks whether it was called
    DeserializerFactory<GenericRecord> trackingDeserializerFactory = (writerSchema, readerSchema) -> {
      deserializerCreated[0] = true;
      return new com.linkedin.venice.serializer.AvroGenericDeserializer<>(writerSchema, readerSchema);
    };

    Client r2Client = getR2Client();
    ClientConfig.ClientConfigBuilder<String, GenericRecord, SpecificRecord> clientConfigBuilder =
        new ClientConfig.ClientConfigBuilder<String, GenericRecord, SpecificRecord>().setStoreName(storeName)
            .setR2Client(r2Client)
            .setValueDeserializerFactory(trackingDeserializerFactory);

    VeniceMetricsRepository metricsRepository = createVeniceMetricsRepository(false);

    AvroGenericStoreClient<String, GenericRecord> fastClient = null;

    try {
      setupStoreMetadata(clientConfigBuilder, StoreMetadataFetchMode.SERVER_BASED_METADATA);
      clientConfigBuilder.setMetricsRepository(metricsRepository);
      ClientConfig clientConfig = clientConfigBuilder.build();

      fastClient = ClientFactory.getAndStartGenericStoreClient(clientConfig);

      // Perform a get operation which should trigger deserializer creation
      GenericRecord value = fastClient.get(keyPrefix + "0").get();
      assertNotNull(value, "Value should not be null");

      // Verify the deserializer factory was called
      assertTrue(deserializerCreated[0], "Deserializer factory should have been invoked");

    } finally {
      if (fastClient != null) {
        fastClient.close();
      }
    }
  }

  /**
   * Test that both custom serializer and deserializer factories work together.
   */
  @Test(timeOut = TIME_OUT)
  public void testBothCustomFactoriesTogether() throws Exception {
    final boolean[] serializerCreated = new boolean[] { false };
    final boolean[] deserializerCreated = new boolean[] { false };

    SerializerFactory<String> trackingSerializerFactory = (keySchema) -> {
      serializerCreated[0] = true;
      return new com.linkedin.venice.serializer.AvroSerializer<>(keySchema);
    };

    DeserializerFactory<GenericRecord> trackingDeserializerFactory = (writerSchema, readerSchema) -> {
      deserializerCreated[0] = true;
      return new com.linkedin.venice.serializer.AvroGenericDeserializer<>(writerSchema, readerSchema);
    };

    Client r2Client = getR2Client();
    ClientConfig.ClientConfigBuilder<String, GenericRecord, SpecificRecord> clientConfigBuilder =
        new ClientConfig.ClientConfigBuilder<String, GenericRecord, SpecificRecord>().setStoreName(storeName)
            .setR2Client(r2Client)
            .setKeySerializerFactory(trackingSerializerFactory)
            .setValueDeserializerFactory(trackingDeserializerFactory);

    VeniceMetricsRepository metricsRepository = createVeniceMetricsRepository(false);

    AvroGenericStoreClient<String, GenericRecord> fastClient = null;

    try {
      setupStoreMetadata(clientConfigBuilder, StoreMetadataFetchMode.SERVER_BASED_METADATA);
      clientConfigBuilder.setMetricsRepository(metricsRepository);
      ClientConfig clientConfig = clientConfigBuilder.build();

      fastClient = ClientFactory.getAndStartGenericStoreClient(clientConfig);

      // Perform operations
      GenericRecord value = fastClient.get(keyPrefix + "0").get();
      assertNotNull(value, "Value should not be null");
      assertEquals((int) value.get(VALUE_FIELD_NAME), 0, "Value should match");

      // Verify both factories were called
      assertTrue(serializerCreated[0], "Serializer factory should have been invoked");
      assertTrue(deserializerCreated[0], "Deserializer factory should have been invoked");

    } finally {
      if (fastClient != null) {
        fastClient.close();
      }
    }
  }

  private Client getR2Client() throws Exception {
    return com.linkedin.venice.fastclient.utils.ClientTestUtils
        .getR2Client(com.linkedin.venice.fastclient.utils.ClientTestUtils.FastClientHTTPVariant.HTTP_2_BASED_R2_CLIENT);
  }
}
