package com.linkedin.venice.controller.kafka.protocol.serializer;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.controller.kafka.protocol.admin.AdminOperation;
import com.linkedin.venice.exceptions.VeniceMessageException;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.utils.Utils;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.specific.SpecificDatumReader;


public class AdminOperationSerializer {
  // Latest schema id, and it needs to be updated whenever we add a new version
  public static final int LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION =
      AvroProtocolDefinition.ADMIN_OPERATION.getCurrentProtocolVersion();

  public static final Schema LATEST_SCHEMA = AdminOperation.getClassSchema();

  /** Used to generate decoders. */
  private static final DecoderFactory DECODER_FACTORY = new DecoderFactory();

  private static final Map<Integer, Schema> PROTOCOL_MAP = initProtocolMap();

  /**
   * Serialize AdminOperation object to bytes.
   *
   * <p>
   * If writerSchemaId equals LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION, return the bytes[] from the first serialization.
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
  public byte[] serialize(AdminOperation object, int writerSchemaId) {
    byte[] serializedBytes = serialize(object, LATEST_SCHEMA);

    // If writerSchema is the latest schema, we can return the serialized bytes directly.
    if (writerSchemaId == LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION) {
      return serializedBytes;
    }

    // Get the writer schema.
    Schema writerSchema = getSchema(writerSchemaId);

    // If writerSchema is not the latest schema, we need to deserialize the serialized bytes to GenericRecord with
    // the writer schema, then serialize it to bytes with the writer schema.
    GenericRecord genericRecord = deserialize(serializedBytes, LATEST_SCHEMA, writerSchema);
    return serialize(genericRecord, writerSchema);
  }

  public AdminOperation deserialize(ByteBuffer byteBuffer, int writerSchemaId) {
    Schema writerSchema = getSchema(writerSchemaId);
    SpecificDatumReader<AdminOperation> reader = new SpecificDatumReader<>(writerSchema, LATEST_SCHEMA);
    Decoder decoder = AvroCompatibilityHelper
        .newBinaryDecoder(byteBuffer.array(), byteBuffer.position(), byteBuffer.remaining(), null);
    try {
      return reader.read(null, decoder);
    } catch (IOException e) {
      throw new VeniceMessageException("Could not deserialize bytes back into AdminOperation object", e);
    }
  }

  public static Map<Integer, Schema> initProtocolMap() {
    try {
      Map<Integer, Schema> protocolSchemaMap = new HashMap<>();
      for (int i = 1; i <= LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION; i++) {
        protocolSchemaMap.put(i, Utils.getSchemaFromResource("avro/AdminOperation/v" + i + "/AdminOperation.avsc"));
      }
      return protocolSchemaMap;
    } catch (IOException e) {
      throw new VeniceMessageException("Could not initialize " + AdminOperationSerializer.class.getSimpleName(), e);
    }
  }

  public static Schema getSchema(int schemaId) {
    if (!PROTOCOL_MAP.containsKey(schemaId)) {
      throw new VeniceMessageException("Admin operation schema version: " + schemaId + " doesn't exist");
    }
    return PROTOCOL_MAP.get(schemaId);
  }

  /**
   * Serialize the object by writer schema
   */
  private <T> byte[] serialize(T object, Schema writerSchema) {
    try {
      GenericDatumWriter<T> datumWriter = new GenericDatumWriter<>(writerSchema);
      ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
      Encoder encoder = AvroCompatibilityHelper.newBinaryEncoder(byteArrayOutputStream, true, null);
      datumWriter.write(object, encoder);
      encoder.flush();
      return byteArrayOutputStream.toByteArray();
    } catch (IOException e) {
      throw new VeniceMessageException("Could not serialize object", e);
    }
  }

  /**
   * Deserialize the object from the writer schema to the reader schema, returning a GenericRecord.
   */
  private GenericRecord deserialize(byte[] serializedRecord, Schema writerSchema, Schema readerSchema) {
    try {
      GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>(writerSchema, readerSchema);
      InputStream in = new ByteArrayInputStream(serializedRecord);
      BinaryDecoder decoder = AvroCompatibilityHelper.newBinaryDecoder(in, true, null);
      return datumReader.read(null, decoder);
    } catch (IOException e) {
      throw new VeniceMessageException("Could not deserialize bytes back into GenericRecord object", e);
    }
  }
}
