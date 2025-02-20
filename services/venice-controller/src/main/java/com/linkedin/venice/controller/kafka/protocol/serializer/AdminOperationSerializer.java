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
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;


public class AdminOperationSerializer {
  // Latest schema id, and it needs to be updated whenever we add a new version
  public static final int LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION =
      AvroProtocolDefinition.ADMIN_OPERATION.getCurrentProtocolVersion();

  private static SpecificDatumWriter<AdminOperation> SPECIFIC_DATUM_WRITER =
      new SpecificDatumWriter<>(AdminOperation.getClassSchema());

  /** Used to generate decoders. */
  private static final DecoderFactory DECODER_FACTORY = new DecoderFactory();

  private static final Map<Integer, Schema> PROTOCOL_MAP = initProtocolMap();

  public byte[] serialize(AdminOperation object, int writerSchemaId) {
    if (writerSchemaId == -1 || writerSchemaId == LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION) {
      return serialize(object, SPECIFIC_DATUM_WRITER);
    } else {
      // Serialize the object to the latest schema
      byte[] serializedBytes = serialize(object, SPECIFIC_DATUM_WRITER);

      // Deserialize the object from the latest schema with the writer schema
      SpecificDatumReader<AdminOperation> specificDatumReader =
          new SpecificDatumReader<>(AdminOperation.getClassSchema(), getSchema(writerSchemaId));
      AdminOperation recordWithLowerSchema = deserialize(serializedBytes, specificDatumReader);

      // Serialize the object to the writer schema
      SpecificDatumWriter<AdminOperation> specificDatumWriter = new SpecificDatumWriter<>(getSchema(writerSchemaId));
      return serialize(recordWithLowerSchema, specificDatumWriter);
    }
  }

  public AdminOperation deserialize(ByteBuffer byteBuffer, int writerSchemaId) {
    Schema writerSchema = getSchema(writerSchemaId);
    SpecificDatumReader<AdminOperation> reader =
        new SpecificDatumReader<>(writerSchema, AdminOperation.getClassSchema());
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

  private byte[] serialize(AdminOperation object, SpecificDatumWriter<AdminOperation> specificDatumWriter) {
    try {
      ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
      Encoder encoder = AvroCompatibilityHelper.newBinaryEncoder(byteArrayOutputStream, true, null);
      specificDatumWriter.write(object, encoder);
      encoder.flush();
      return byteArrayOutputStream.toByteArray();
    } catch (IOException e) {
      throw new VeniceMessageException("Could not serialize AdminOperation object", e);
    }
  }

  private AdminOperation deserialize(byte[] serializedRecord, SpecificDatumReader<AdminOperation> specificDatumReader) {
    try {
      InputStream in = new ByteArrayInputStream(serializedRecord);
      BinaryDecoder decoder = AvroCompatibilityHelper.newBinaryDecoder(in, true, null);
      return specificDatumReader.read(null, decoder);
    } catch (IOException e) {
      throw new VeniceMessageException("Could not deserialize bytes back into AdminOperation object", e);
    }
  }
}
