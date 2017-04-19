package com.linkedin.venice.controller.kafka.protocol.serializer;

import com.linkedin.venice.controller.kafka.protocol.admin.AdminOperation;
import com.linkedin.venice.exceptions.VeniceMessageException;
import com.linkedin.venice.utils.Utils;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class AdminOperationSerializer {
  // Latest schema id, and it needs to be updated whenever we add a new version
  public static int LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION = 5;

  private static SpecificDatumWriter<AdminOperation> SPECIFIC_DATUM_WRITER = new SpecificDatumWriter<>(AdminOperation.SCHEMA$);
  /** Used to generate decoders. */
  private static final DecoderFactory DECODER_FACTORY = new DecoderFactory();

  private static final Map<Integer, Schema> PROTOCOL_MAP = initProtocolMap();

  public byte[] serialize(AdminOperation object) {
    try {
      ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
      Encoder encoder = new BinaryEncoder(byteArrayOutputStream);
      SPECIFIC_DATUM_WRITER.write(object, encoder);

      return byteArrayOutputStream.toByteArray();
    } catch (IOException e) {
      throw new VeniceMessageException("Failed to encode message: " + object.toString(), e);
    }
  }

  public AdminOperation deserialize(byte[] bytes, int writerSchemaId) {
    if (!PROTOCOL_MAP.containsKey(writerSchemaId)) {
      throw new VeniceMessageException("Writer schema: " + writerSchemaId + " doesn't exist");
    }
    SpecificDatumReader<AdminOperation> reader = new SpecificDatumReader<>(
        PROTOCOL_MAP.get(writerSchemaId), AdminOperation.SCHEMA$);
    Decoder decoder = DECODER_FACTORY.createBinaryDecoder(bytes, null);
    try {
      return reader.read(null, decoder);
    } catch(IOException e) {
      throw new VeniceMessageException("Could not deserialize bytes back into AdminOperation object" + e);
    }
  }

  private static Map<Integer, Schema> initProtocolMap() {
    try {
      Map<Integer, Schema> protocolSchemaMap = new HashMap<>();
      // TODO reuse some codes from InternalAvroSpecificSerializer to initialize all versions of schema.
      protocolSchemaMap.put(1, Utils.getSchemaFromResource("avro/AdminOperation/v1/AdminOperation.avsc"));
      protocolSchemaMap.put(2, Utils.getSchemaFromResource("avro/AdminOperation/v2/AdminOperation.avsc"));
      protocolSchemaMap.put(3, Utils.getSchemaFromResource("avro/AdminOperation/v3/AdminOperation.avsc"));
      protocolSchemaMap.put(4, Utils.getSchemaFromResource("avro/AdminOperation/v4/AdminOperation.avsc"));
      protocolSchemaMap.put(5, Utils.getSchemaFromResource("avro/AdminOperation/v5/AdminOperation.avsc"));
      // TODO: If we add more versions to the protocol, they should be initialized here.

      return protocolSchemaMap;
    } catch (IOException e) {
      throw new VeniceMessageException("Could not initialize " + AdminOperationSerializer.class.getSimpleName(), e);
    }
  }
}
