package com.linkedin.venice.serialization.avro;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceMessageException;
import com.linkedin.venice.serialization.VeniceKafkaSerializer;
import com.linkedin.venice.utils.ByteUtils;
import com.linkedin.venice.utils.Utils;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.LinkedinAvroMigrationHelper;
import org.apache.avro.specific.SpecificData;
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
 * 2nd byte: The protocol version
 * 3rd byte and onward: The payload (a single binary-encoded Avro record) encoded
 *    with a writer schema determined by the protocol version specified in #2.
 */
public class InternalAvroSpecificSerializer<SPECIFIC_RECORD extends SpecificRecord>
    implements VeniceKafkaSerializer<SPECIFIC_RECORD> {

  private static final Logger logger = Logger.getLogger(InternalAvroSpecificSerializer.class);
  private static final int SENTINEL_PROTOCOL_VERSION_USED_FOR_UNDETECTABLE_COMPILED_SCHEMA = -1;
  private static final int SENTINEL_PROTOCOL_VERSION_USED_FOR_UNVERSIONED_PROTOCOL = 0;

  // Constants related to the protocol definition:

  // (Optional 1st byte) Magic Byte
  private static final int MAGIC_BYTE_OFFSET = 0;
  private final int MAGIC_BYTE_LENGTH;
  private final byte magicByte;

  // (Optional 2nd byte) Protocol version
  private final int PROTOCOL_VERSION_OFFSET;
  private final int PROTOCOL_VERSION_LENGTH;
  private final byte currentProtocolVersion;

  // 1st or 3rd byte and onward: Payload (a single binary-encoded Avro record)
  private final int PAYLOAD_OFFSET;

  // Re-usable Avro facilities. Can be shared across multiple threads, so we only need one per process.

  /** Used to generate decoders. */
  private static final DecoderFactory DECODER_FACTORY = new DecoderFactory();

  /** Used to serialize objects into binary-encoded Avro according to the latest protocol version. */
  private final SpecificDatumWriter writer;

  /** Maintains the mapping between protocol version and the corresponding {@link SpecificDatumReader<SPECIFIC_RECORD>} */
  private final Map<Integer, SpecificDatumReader<SPECIFIC_RECORD>> readerMap = new HashMap<>();

  protected InternalAvroSpecificSerializer(AvroProtocolDefinition protocolDef) {
    this(protocolDef, null);
  }
  protected InternalAvroSpecificSerializer(AvroProtocolDefinition protocolDef, Integer payloadOffsetOverride) {

    // Magic byte handling
    if (protocolDef.magicByte.isPresent()) {
      this.magicByte = protocolDef.magicByte.get();
      this.MAGIC_BYTE_LENGTH = 1;
    } else {
      this.magicByte = 0;
      this.MAGIC_BYTE_LENGTH = 0;
    }

    // Protocol version handling
    this.PROTOCOL_VERSION_OFFSET = MAGIC_BYTE_OFFSET + MAGIC_BYTE_LENGTH;
    if (protocolDef.protocolVersionStoredInHeader) {
      this.PROTOCOL_VERSION_LENGTH = 1;
    } else {
      this.PROTOCOL_VERSION_LENGTH = 0;
    }
    if (protocolDef.currentProtocolVersion.isPresent()) {
      int currentProtocolVersionAsInt = protocolDef.currentProtocolVersion.get();
      if (currentProtocolVersionAsInt == SENTINEL_PROTOCOL_VERSION_USED_FOR_UNDETECTABLE_COMPILED_SCHEMA ||
          currentProtocolVersionAsInt == SENTINEL_PROTOCOL_VERSION_USED_FOR_UNVERSIONED_PROTOCOL ||
          currentProtocolVersionAsInt > Byte.MAX_VALUE) {
        throw new IllegalArgumentException("Improperly defined protocol! Invalid currentProtocolVersion: " + currentProtocolVersionAsInt);
      }
      this.currentProtocolVersion = (byte) currentProtocolVersionAsInt;
    } else {
      this.currentProtocolVersion = SENTINEL_PROTOCOL_VERSION_USED_FOR_UNVERSIONED_PROTOCOL;
    }

    // Payload handling
    if (null == payloadOffsetOverride) {
      this.PAYLOAD_OFFSET = PROTOCOL_VERSION_OFFSET + PROTOCOL_VERSION_LENGTH;
    } else {
      if (protocolDef.magicByte.isPresent() || protocolDef.protocolVersionStoredInHeader) {
        throw new VeniceMessageException("The payload offset override is not intended to be used for protocols which have explicitly defined magic bytes or which protocol versions stored in their header.");
      }
      this.PAYLOAD_OFFSET = payloadOffsetOverride;
    }
    this.writer = initializeAvroSpecificDatumReaderAndWriter(protocolDef);
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
      Encoder encoder = LinkedinAvroMigrationHelper.newBinaryEncoder(byteArrayOutputStream);

      // We write according to the latest protocol version.
      if (MAGIC_BYTE_LENGTH == 1) {
        byteArrayOutputStream.write(magicByte);
      }
      if (PROTOCOL_VERSION_LENGTH == 1) {
        byteArrayOutputStream.write(currentProtocolVersion);
      }
      writer.write(object, encoder);
      encoder.flush();
      return byteArrayOutputStream.toByteArray();
    } catch (IOException e) {
      throw new VeniceMessageException(this.getClass().getSimpleName() + " failed to encode message: " + object.toString(), e);
    }
  }

  /**
   * Create an object from an array of bytes
   *
   * This method is used by the Kafka consumer. These calls are always intended to be for protocols
   * which use a magic byte and a protocol version, both of which are stored in the header, before
   * the payload.
   *
   * @param topic Topic to which the array of bytes belongs.
   * @param bytes An array of bytes representing the object's data serialized in Avro binary format.
   * @return A {@link SPECIFIC_RECORD} serialized from the bytes
   */
  @Override
  public SPECIFIC_RECORD deserialize(String topic, byte[] bytes) {
    if (bytes == null || bytes.length < PAYLOAD_OFFSET) {
      throw new IllegalArgumentException("Invalid byte array for serialization - no bytes to read");
    }

    if (magicByte == 0) {
      throw new VeniceMessageException("This protocol cannot be used as a Kafka deserializer: " + this.getClass().getSimpleName());
    }

    // Sanity check on the magic byte to make sure we understand the protocol itself
    if (bytes[MAGIC_BYTE_OFFSET] != magicByte) {
      throw new VeniceMessageException(
          "Received Magic Byte '" + new String(bytes, MAGIC_BYTE_OFFSET, MAGIC_BYTE_LENGTH)
              + "' which is not supported by " + this.getClass().getSimpleName()
              + ". The only supported Magic Byte for this implementation is '" + magicByte + "'.");
    }

    if (PROTOCOL_VERSION_LENGTH == 0) {
      throw new VeniceMessageException("This protocol cannot be used as a Kafka deserializer: " + this.getClass().getSimpleName());
    }

    // If the data looks valid, then we deploy the Avro machinery to decode the payload

    return deserialize(bytes, bytes[PROTOCOL_VERSION_OFFSET]);
  }

  public SPECIFIC_RECORD deserialize(byte[] bytes, int protocolVersion) {
    if (bytes == null || bytes.length < PAYLOAD_OFFSET) {
      throw new IllegalArgumentException("Invalid byte array for serialization - no bytes to read");
    }

    // Sanity check to make sure the writer's protocol (i.e.: Avro schema) version is known to us
    if (!readerMap.containsKey(protocolVersion)) {
      throw new VeniceMessageException(
          "Received Protocol Version '" + protocolVersion
              + "' which is not supported by " + this.getClass().getSimpleName()
              + ". The only supported Protocol Versions are [" + readerMap.keySet()
              .stream()
              .sorted()
              .map(b -> b.toString())
              .collect(Collectors.joining(", ")) + "].");
    }

    try {
      /**
       * Reuse SpecificDatumReader since it is thread-safe, and generating a brand new SpecificDatumReader is very slow
       * sometimes.
       *
       * When generating a new {@link SpecificDatumReader}, both reader schema and writer schema need to be setup, and
       * internally {@link SpecificDatumReader} needs to calculate {@link org.apache.avro.io.ResolvingDecoder}, but
       * the slowest part is to persist those info to thread-local variables, which could take tens of seconds sometimes.
       *
       * TODO: investigate why {@link ThreadLocal} operations (internally {@link java.lang.ThreadLocal.ThreadLocalMap})
       * are so slow sometimes.
       */

      SpecificDatumReader<SPECIFIC_RECORD> specificDatumReader = readerMap.get(protocolVersion);

      Decoder decoder = createBinaryDecoder(
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
      throw new VeniceMessageException(this.getClass().getSimpleName() + " failed to decode message from: " + ByteUtils.toHexString(bytes), e);
    }
  }

  protected BinaryDecoder createBinaryDecoder(byte[] bytes, int offset,
      int length, BinaryDecoder reuse) {
    return DECODER_FACTORY.createBinaryDecoder(bytes, offset, length, reuse);
  }

  protected SpecificDatumReader<SPECIFIC_RECORD> createSpecificDatumReader(Schema writer, Schema reader) {
    return new SpecificDatumReader<>(writer, reader);
  }

  /**
   * Initialize both {@link #readerMap} and {@link #writer}.
   *
   * @param protocolDef
   */
  private SpecificDatumWriter initializeAvroSpecificDatumReaderAndWriter(AvroProtocolDefinition protocolDef) {
    Schema compiledProtocol = protocolDef.schema;
    byte compiledProtocolVersion = SENTINEL_PROTOCOL_VERSION_USED_FOR_UNDETECTABLE_COMPILED_SCHEMA;
    String className = protocolDef.className;
    Map<Integer, Schema> protocolSchemaMap = new HashMap<>();
    int initialVersion;
    if (currentProtocolVersion > 0) {
      initialVersion = 1; // TODO: Consider making configurable if we ever need to fully deprecate some old versions
    } else {
      initialVersion = currentProtocolVersion;
    }
    final String sep = "/"; // TODO: Make sure that jar resources are always forward-slash delimited, even on Windows
    int version = initialVersion;
    while (true) {
      String versionPath = "avro" + sep + className + sep;
      if (this.currentProtocolVersion != SENTINEL_PROTOCOL_VERSION_USED_FOR_UNVERSIONED_PROTOCOL) {
        versionPath += "v" + version + sep;
      }
      versionPath += className + ".avsc";
      try {
        Schema schema = Utils.getSchemaFromResource(versionPath);
        protocolSchemaMap.put(version, schema);
        if (schema.equals(compiledProtocol)) {
          compiledProtocolVersion = (byte) version;
        }
        if (this.currentProtocolVersion == SENTINEL_PROTOCOL_VERSION_USED_FOR_UNVERSIONED_PROTOCOL) {
          break;
        } else if (this.currentProtocolVersion > 0) {
          // Positive version protocols should continue looking "up" for the next version
          version++;
        } else {
          // And vice-versa for negative version protocols
          version--;
        }
      } catch (IOException e) {
        // Then the schema was not found at the requested path
        if (version == initialVersion) {
          throw new VeniceException("Failed to initialize schemas! No resource found at: " + versionPath, e);
        } else {
          break;
        }
      }
    }

    /** Ensure that we are using Avro properly. */
    if (compiledProtocolVersion == SENTINEL_PROTOCOL_VERSION_USED_FOR_UNDETECTABLE_COMPILED_SCHEMA) {
      throw new VeniceException("Failed to identify which version is currently compiled for " + protocolDef.name() +
          ". This could happen if the avro schemas have been altered without recompiling the auto-generated classes" +
          ", or if the auto-generated classes were edited directly instead of generating them from the schemas.");
    }

    /**
     * Verify that the intended current protocol version defined in the {@link AvroProtocolDefinition} is available
     * in the jar's resources and that it matches the auto-generated class that is actually compiled.
     *
     * N.B.: An alternative design would have been to assume that what is compiled is the intended version, but we
     * are instead making this a very explicit choice by requiring the change in both places and failing loudly
     * when there is an inconsistency.
     */
    Schema intendedCurrentProtocol = protocolSchemaMap.get((int) this.currentProtocolVersion);
    if (null == intendedCurrentProtocol) {
      throw new VeniceException("Failed to get schema for current version: " + this.currentProtocolVersion
          + " class: " + className);
    } else if (!intendedCurrentProtocol.equals(compiledProtocol)) {
      throw new VeniceException("The intended protocol version (" + this.currentProtocolVersion +
          ") does not match the compiled protocol version (" + compiledProtocolVersion + ").");
    }

    /** Initialize {@link #readerMap} based on known protocol versions */
    for (Map.Entry<Integer, Schema> entry : protocolSchemaMap.entrySet()) {
      SpecificDatumReader<SPECIFIC_RECORD> specificDatumReader = createSpecificDatumReader(entry.getValue(), compiledProtocol);
      this.readerMap.put(entry.getKey(), specificDatumReader);
    }

    return new SpecificDatumWriter(protocolDef.schema);
  }
}
