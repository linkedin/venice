package com.linkedin.venice.controller.kafka.protocol.serializer;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.annotation.VisibleForTesting;
import com.linkedin.venice.controller.VeniceHelixAdmin;
import com.linkedin.venice.controller.kafka.protocol.admin.AdminOperation;
import com.linkedin.venice.exceptions.VeniceProtocolException;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.Encoder;
import org.apache.avro.specific.SpecificDatumReader;


public class AdminOperationSerializer {
  // Latest schema id, and it needs to be updated whenever we add a new version
  public static final int LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION =
      AvroProtocolDefinition.ADMIN_OPERATION.getCurrentProtocolVersion();

  private static final Schema LATEST_SCHEMA = AdminOperation.getClassSchema();

  private static final Map<Integer, Schema> PROTOCOL_MAP = initProtocolMap();

  /**
   * Cache for schemas downloaded from system store schema repository.
   * This map is separate from PROTOCOL_MAP to distinguish between built-in schemas and downloaded schemas.
   * Built-in schemas are initialized at startup and are immutable.
   * Downloaded schemas are downloaded from system store schema repository, and are mutable.
   */
  private static final Map<Integer, Schema> cacheSchemaMapFromSystemStore = new VeniceConcurrentHashMap<>();

  /**
   * Serialize AdminOperation object to bytes[] with the writer schema
   * @param object AdminOperation object
   * @param targetSchemaId writer schema id that we will refer to for serialization and deserialization
   *
   * <p>
   * If targetSchemaId equals LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION, return the bytes[] from the first serialization.
   * Otherwise, serialize the object to the writer schema (lower version).
   * </p>
   *
   * <p>
   * This involves:
   * <ol>
   *   <li>Serializing the object to a GenericRecord with the latest schema.</li>
   *   <li>Deserializing it to a GenericRecord with the writer schema.</li>
   *   <li>Serializing it to bytes.</li>
   * </ol>
   * </p>
   * <p>
   * This process ensures the object is serialized to the lower schema version.
   * The normal serialization process may fail (ClassCastException) due to:
   * <ul>
   *   <li>Differences in field types</li>
   *   <li>New fields added in the middle of the schema instead of at the end</li>
   * </ul>
   * </p>
   */
  public byte[] serialize(AdminOperation object, int targetSchemaId) {
    byte[] serializedBytes = serialize(object, LATEST_SCHEMA, LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);

    // If writerSchema is the latest schema, we can return the serialized bytes directly.
    if (targetSchemaId == LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION) {
      return serializedBytes;
    }

    // Get the writer schema.
    Schema targetSchema = getSchema(targetSchemaId);

    // If writer schema is not the latest schema, we need to deserialize the serialized bytes to GenericRecord with
    // the writer schema, then serialize it to bytes with the writer schema.
    try {
      GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>(LATEST_SCHEMA, targetSchema);
      InputStream in = new ByteArrayInputStream(serializedBytes);
      BinaryDecoder decoder = AvroCompatibilityHelper.newBinaryDecoder(in, true, null);
      GenericRecord genericRecord = datumReader.read(null, decoder);
      return serialize(genericRecord, targetSchema, targetSchemaId);
    } catch (IOException e) {
      throw new VeniceProtocolException(
          "Could not deserialize bytes back into GenericRecord object with reader version: " + targetSchema,
          e);
    }
  }

  public AdminOperation deserialize(ByteBuffer byteBuffer, int writerSchemaId) {
    Schema writerSchema = getSchema(writerSchemaId);
    SpecificDatumReader<AdminOperation> reader = new SpecificDatumReader<>(writerSchema, LATEST_SCHEMA);
    Decoder decoder = AvroCompatibilityHelper
        .newBinaryDecoder(byteBuffer.array(), byteBuffer.position(), byteBuffer.remaining(), null);
    try {
      return reader.read(null, decoder);
    } catch (IOException e) {
      throw new VeniceProtocolException(
          "Could not deserialize bytes back into AdminOperation object with schema id: " + writerSchemaId,
          e);
    }
  }

  /**
   * Validate the AdminOperation message against the target schema.
   * @throws VeniceProtocolException if the message does not conform to the target schema.
   */
  public void validate(AdminOperation message, int targetSchemaId) {
    Schema targetSchema = getSchema(targetSchemaId);
    try {
      SemanticDetector.traverseAndValidate(message, LATEST_SCHEMA, targetSchema, "AdminOperation", null);
    } catch (VeniceProtocolException e) {
      throw new VeniceProtocolException(
          String.format("Current schema version: %s. New semantic is being used. %s", targetSchemaId, e.getMessage()),
          e);
    }
  }

  public static Map<Integer, Schema> initProtocolMap() {
    try {
      Map<Integer, Schema> protocolSchemaMap = new VeniceConcurrentHashMap<>();
      for (int i = 1; i <= LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION; i++) {
        protocolSchemaMap.put(i, Utils.getSchemaFromResource("avro/AdminOperation/v" + i + "/AdminOperation.avsc"));
      }
      return protocolSchemaMap;
    } catch (IOException e) {
      throw new VeniceProtocolException("Could not initialize " + AdminOperationSerializer.class.getSimpleName(), e);
    }
  }

  public Schema getSchema(int schemaId) {
    if (PROTOCOL_MAP.containsKey(schemaId)) {
      return PROTOCOL_MAP.get(schemaId);
    }
    if (cacheSchemaMapFromSystemStore.containsKey(schemaId)) {
      return cacheSchemaMapFromSystemStore.get(schemaId);
    }

    throw new VeniceProtocolException("Admin operation schema version: " + schemaId + " doesn't exist");
  }

  /**
   * Download schema from system store schema repository and add it to the protocol map if not already present.
   * @throws VeniceProtocolException if the schema could not be found in the system store schema repository.
   */
  public void fetchAndStoreSchemaIfAbsent(VeniceHelixAdmin admin, int schemaId) {
    // No need to download if the schema is already available.
    if (PROTOCOL_MAP.containsKey(schemaId) || cacheSchemaMapFromSystemStore.containsKey(schemaId)) {
      return;
    }
    String adminOperationSchemaStoreName = AvroProtocolDefinition.ADMIN_OPERATION.getSystemStoreName();
    Schema schema =
        admin.getReadOnlyZKSharedSchemaRepository().getValueSchema(adminOperationSchemaStoreName, schemaId).getSchema();
    if (schema == null) {
      throw new VeniceProtocolException(
          "Could not find AdminOperation schema for schema id: " + schemaId + " in system store schema repository");
    }
    cacheSchemaMapFromSystemStore.put(schemaId, schema);
  }

  /**
   * Serialize the object by writer schema
   */
  private <T> byte[] serialize(T object, Schema writerSchema, int writerSchemaId) {
    try {
      GenericDatumWriter<T> datumWriter = new GenericDatumWriter<>(writerSchema);
      ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
      Encoder encoder = AvroCompatibilityHelper.newBinaryEncoder(byteArrayOutputStream, true, null);
      datumWriter.write(object, encoder);
      encoder.flush();
      return byteArrayOutputStream.toByteArray();
    } catch (IOException e) {
      throw new VeniceProtocolException(
          "Could not serialize object: " + object.getClass().getTypeName() + " with writer schema id: "
              + writerSchemaId,
          e);
    }
  }

  @VisibleForTesting
  public void addSchema(int schemaId, Schema schema) {
    cacheSchemaMapFromSystemStore.put(schemaId, schema);
  }

  @VisibleForTesting
  public void removeSchema(int schemaId) {
    cacheSchemaMapFromSystemStore.remove(schemaId);
  }
}
