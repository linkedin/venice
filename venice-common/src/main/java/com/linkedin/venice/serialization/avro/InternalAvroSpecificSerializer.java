package com.linkedin.venice.serialization.avro;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceMessageException;
import com.linkedin.venice.serialization.VeniceSerializer;
import com.linkedin.venice.utils.ByteUtils;
import com.linkedin.venice.utils.Utils;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.apache.log4j.Logger;


/**
 * Serializer for translating a versioned protocol of Avro records.
 *
 * The protocol is the following:
 *
 * 1st byte: The magic byte, should always equal '{@link #magicByte}'.
 * 2nd byte: The protocol version, currently, only '1' is supported.
 * 3rd byte and onward: The payload (a single binary-encoded Avro record) encoded
 *    with a writer schema determined by the protocol version specified in #2.
 */
abstract public class InternalAvroSpecificSerializer<SPECIFIC_RECORD extends SpecificRecord>
    implements VeniceSerializer<SPECIFIC_RECORD> {

  private static final Logger logger = Logger.getLogger(InternalAvroSpecificSerializer.class);

  // Constants related to the protocol definition:

  // 1st byte: Magic Byte
  private static final int MAGIC_BYTE_OFFSET = 0;
  private static final int MAGIC_BYTE_LENGTH = 1;
  private final byte magicByte;

  // 2nd byte: Protocol version
  private static final int PROTOCOL_VERSION_OFFSET = MAGIC_BYTE_OFFSET + MAGIC_BYTE_LENGTH;
  private static final int PROTOCOL_VERSION_LENGTH = 1;
  private final byte currentProtocolVersion;

  // 3rd byte and onward: Payload (a single binary-encoded Avro record)
  private static final int PAYLOAD_OFFSET = PROTOCOL_VERSION_OFFSET + PROTOCOL_VERSION_LENGTH;

  // Re-usable Avro facilities. Can be shared across multiple threads, so we only need one per process.

  /** Used to generate decoders. */
  private static final DecoderFactory DECODER_FACTORY = new DecoderFactory();

  /** Used to serialize objects into binary-encoded Avro according to the latest protocol version. */
  private final SpecificDatumWriter specificDatumWriter;

  /** Holds the mapping of protocol version to {@link Schema} instance. */
  private final Map<Byte, Schema> protocols;

  protected InternalAvroSpecificSerializer(AvroProtocolDefinition protocolDef) {
    this.magicByte = protocolDef.magicByte;
    this.currentProtocolVersion = protocolDef.currentProtocolVersion;
    this.protocols = initializeProtocolSchemaMap(protocolDef.specificRecordClass.getSimpleName());
    this.specificDatumWriter = new SpecificDatumWriter(protocols.get(currentProtocolVersion));
  }

  /**
   * Close this serializer.
   * This method has to be idempotent if the serializer is used in KafkaProducer because it might be called
   * multiple times.
   */
  @Override
  public void close() {

  }

  /**
   * Configure this class.
   *
   * @param configMap configs in key/value pairs
   * @param isKey     whether is for key or value
   */
  @Override
  public void configure(Map<String, ?> configMap, boolean isKey) {
    if (isKey) {
      throw new VeniceException("Cannot use " + getClass().getSimpleName() + " for key data.");
    }
  }

  /**
   * Construct an array of bytes from the given object
   *
   * @param topic  Topic to which the object belongs.
   * @param object A {@link SPECIFIC_RECORD} instance to be serialized.
   * @return The Avro binary format bytes which represent the {@param object}
   */
  @Override
  public byte[] serialize(String topic, SPECIFIC_RECORD object) {
    try {
      // If single-threaded, both the ByteArrayOutputStream and Encoder can be re-used. TODO: explore GC tuning later.
      ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
      Encoder encoder = new BinaryEncoder(byteArrayOutputStream);

      // We write according to the latest protocol version.
      byteArrayOutputStream.write(magicByte);
      byteArrayOutputStream.write(currentProtocolVersion);
      specificDatumWriter.write(object, encoder);

      return byteArrayOutputStream.toByteArray();
    } catch (IOException e) {
      throw new VeniceMessageException("Failed to encode message from topic '" + topic + "': " + object.toString(), e);
    }
  }

  /**
   * Create an object from an array of bytes
   *
   * @param topic Topic to which the array of bytes belongs.
   * @param bytes An array of bytes representing the object's data serialized in Avro binary format.
   * @return A {@link SPECIFIC_RECORD} serialized from the bytes
   */
  @Override
  public SPECIFIC_RECORD deserialize(String topic, byte[] bytes) {
    try {
      if (bytes == null || bytes.length < PAYLOAD_OFFSET) {
        throw new IllegalArgumentException("Invalid byte array for serialization - no bytes to read");
      }

      // Sanity check on the magic byte to make sure we understand the protocol itself
      if (bytes[MAGIC_BYTE_OFFSET] != magicByte) {
        throw new VeniceMessageException("Received Magic Byte '" +
            new String(bytes, MAGIC_BYTE_OFFSET, MAGIC_BYTE_LENGTH) +
            "' which is not supported by " + this.getClass().getSimpleName() +
            ". The only supported Magic Byte for this implementation is '" + magicByte + "'.");
      }

      // Sanity check to make sure the writer's protocol (i.e.: Avro schema) version is known to us
      if (!protocols.containsKey(bytes[PROTOCOL_VERSION_OFFSET])) {
        throw new VeniceMessageException("Received Protocol Version '" +
            new String(bytes, PROTOCOL_VERSION_OFFSET, PROTOCOL_VERSION_LENGTH) +
            "' which is not supported by " + this.getClass().getSimpleName() +
            ". The only supported Protocol Versions are [" +
            protocols.keySet().stream().sorted().map(b -> b.toString()).collect(Collectors.joining(", ")) + "].");
      }

      // If the data looks valid, then we deploy the Avro machinery to decode the payload

      // If single-threaded, DatumReader can be re-used. TODO: explore GC tuning later.
      SpecificDatumReader<SPECIFIC_RECORD> specificDatumReader = new SpecificDatumReader<>();

      specificDatumReader.setSchema(protocols.get(bytes[PROTOCOL_VERSION_OFFSET])); // Writer's schema
      specificDatumReader.setExpected(protocols.get(currentProtocolVersion));       // Reader's schema

      Decoder decoder = DECODER_FACTORY.createBinaryDecoder(
          bytes,                         // The bytes array we wish to decode
          PAYLOAD_OFFSET,                // Where to start reading from in the bytes array
          bytes.length - PAYLOAD_OFFSET, // The length to read in the bytes array
          null                           // This param is to re-use a Decoder instance. TODO: explore GC tuning later.
      );

      SPECIFIC_RECORD record = specificDatumReader.read(
          null, // This param is to re-use a SPECIFIC_RECORD instance. TODO: explore GC tuning later.
          decoder
      );

      return record;
    } catch (IOException e) {
      throw new VeniceMessageException("Failed to decode message from '" + topic + "': " + ByteUtils.toHexString(bytes), e);
    }
  }

  /**
   * We initialize the protocols map once per process, the first time we construct an instance of this class.
   */
  private static Map<Byte, Schema> initializeProtocolSchemaMap(String className) {
    Map protocolSchemaMap = new HashMap<>();
    final int initialVersion = 1; // TODO: Consider making configurable if we ever need to fully deprecate some old versions
    final String sep = "/"; // TODO: Make sure that jar resources are always forward-slash delimited, even on Windows
    int version = initialVersion;
    while (true) {
      String versionPath = "avro" + sep + className + sep + "v" + version + sep + className + ".avsc";
      try {
        Schema schema = Utils.getSchemaFromResource(versionPath);
        protocolSchemaMap.put((byte) version, schema);
        version++;
      } catch (IOException e) {
        // Then the schema was not found at the requested path
        if (version == initialVersion) {
          throw new VeniceException("Failed to initialize schemas! No resource found at: " + versionPath, e);
        } else {
          break;
        }
      }
    }

    return protocolSchemaMap;
  }
}
