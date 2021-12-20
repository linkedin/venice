package com.linkedin.venice.serialization.avro;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.client.schema.SchemaReader;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceMessageException;
import com.linkedin.venice.serialization.VeniceKafkaSerializer;
import com.linkedin.venice.utils.ByteUtils;
import com.linkedin.venice.utils.Utils;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


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

  /** Used to configure the {@link #schemaReader}. */
  public static final String VENICE_SCHEMA_READER_CONFIG = "venice.schema-reader";

  private static final Logger logger = LogManager.getLogger(InternalAvroSpecificSerializer.class);
  public static final int MAX_ATTEMPTS_FOR_SCHEMA_READER = 60;
  public static final int WAIT_TIME_BETWEEN_SCHEMA_READER_ATTEMPTS_IN_MS = 1000;
  public static final int SENTINEL_PROTOCOL_VERSION_USED_FOR_UNDETECTABLE_COMPILED_SCHEMA = -1;
  public static final int SENTINEL_PROTOCOL_VERSION_USED_FOR_UNVERSIONED_PROTOCOL = 0;

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
  private final Map<Integer, VeniceSpecificDatumReader<SPECIFIC_RECORD>> readerMap = new HashMap<>();

  /** The schema of the {@link SpecificRecord} which is compiled in the current version of the code. */
  private final Schema compiledProtocol;

  /** Used to fetch unknown schemas, to ensure forward compatibility when the protocol gets upgraded. */
  private SchemaReader schemaReader = null;

  protected InternalAvroSpecificSerializer(AvroProtocolDefinition protocolDef) {
    this(protocolDef, null);
  }

  protected InternalAvroSpecificSerializer(AvroProtocolDefinition protocolDef, Integer payloadOffsetOverride) {
    // Magic byte handling
    if (protocolDef.getMagicByte().isPresent()) {
      this.magicByte = protocolDef.getMagicByte().get();
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
        throw new VeniceMessageException("The payload offset override is not intended to be used for protocols "
            + "which have explicitly defined magic bytes or which have protocol versions stored in their header.");
      }
      this.PAYLOAD_OFFSET = payloadOffsetOverride;
    }
    this.compiledProtocol = protocolDef.getCurrentProtocolVersionSchema();
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

  public Set<Integer> knownProtocols() {
    return readerMap.keySet();
  }

  /**
   * Configure this class. {@link org.apache.kafka.clients.consumer.KafkaConsumer#KafkaConsumer(Properties)} would
   * eventually call {@link #configure(Map, boolean)} which would pass in the customized Kafka config map with schema
   * reader.
   *
   * @param configMap configs in key/value pairs
   * @param isKey     whether is for key or value
   */
  @Override
  public void configure(Map<String, ?> configMap, boolean isKey) {
    if (isKey) {
      throw new VeniceException("Cannot use " + getClass().getSimpleName() + " for key data.");
    }

    if (configMap.containsKey(VENICE_SCHEMA_READER_CONFIG)) {
      this.schemaReader = (SchemaReader) configMap.get(VENICE_SCHEMA_READER_CONFIG);
      logger.info("Serializer has schemaReader: " + schemaReader);
    } else {
      logger.info("Serializer doesn't have schemaReader");
    }
  }

  public void setSchemaReader(SchemaReader schemaReader) {
    this.schemaReader = schemaReader;
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
    // If single-threaded, both the ByteArrayOutputStream and Encoder can be re-used. TODO: explore GC tuning later.
    try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream()) {
      Encoder encoder = AvroCompatibilityHelper.newBinaryEncoder(byteArrayOutputStream, true, null);

      // We write according to the latest protocol version.
      if (MAGIC_BYTE_LENGTH == 1) {
        byteArrayOutputStream.write(magicByte);
      }
      if (PROTOCOL_VERSION_LENGTH == 1) {
        byteArrayOutputStream.write(currentProtocolVersion);
      }
      if (byteArrayOutputStream.size() < PAYLOAD_OFFSET) {
        // In some code paths, we override the payload offset so that we can have some padding at the beginning.
        // N.B. The size of byteArrayOutputStream increases after writing a byte. We should use a fixed number
        // in the termination expression instead of PAYLOAD_OFFSET - byteArrayOutputStream.size(), which terminates
        // the loop earlier
        int paddingSize = PAYLOAD_OFFSET - byteArrayOutputStream.size();
        for (int i = 0; i < paddingSize; i++) {
          byteArrayOutputStream.write(0);
        }
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

  // TODO (lcli): can be enhanced in the future to take a ByteBuffer input to avoid unnecessary array copy
  public SPECIFIC_RECORD deserialize(byte[] bytes, int protocolVersion) {
    if (bytes == null || bytes.length < PAYLOAD_OFFSET) {
      throw new IllegalArgumentException("Invalid byte array for serialization - no bytes to read");
    }

    // Sanity check to make sure the writer's protocol (i.e.: Avro schema) version is known to us
    if (!readerMap.containsKey(protocolVersion)) {
      if (null == schemaReader) {
        throw new VeniceMessageException("Received Protocol Version '" + protocolVersion
            + "' which is not supported by " + this.getClass().getSimpleName()
            + ". Protocol forward compatibility is not enabled"
            + ". The only supported Protocol Versions are: " + getCurrentlyLoadedProtocolVersions() + ".");
      }

      for (int attempt = 1; attempt <= MAX_ATTEMPTS_FOR_SCHEMA_READER; attempt++) {
        try {
          Schema newProtocolSchema = schemaReader.getValueSchema(protocolVersion);
          if (null == newProtocolSchema) {
            throw new VeniceMessageException("Received Protocol Version '" + protocolVersion
                + "' which is not currently known by " + this.getClass().getSimpleName()
                + ". A remote fetch was attempted, but the " + SchemaReader.class.getSimpleName() + " returned null"
                + ". The currently known Protocol Versions are: " + getCurrentlyLoadedProtocolVersions() + ".");
          }

          cacheDatumReader(protocolVersion, newProtocolSchema);

          logger.info("Discovered new protocol version '" + protocolVersion + "', and successfully retrieved it. Schema:\n"
              + newProtocolSchema.toString(true));

          break;
        } catch (Exception e) {
          if (attempt == MAX_ATTEMPTS_FOR_SCHEMA_READER) {
            throw new VeniceException("Failed to retrieve new protocol schema version (" + protocolVersion + ") after "
                + MAX_ATTEMPTS_FOR_SCHEMA_READER + " attempts.", e);
          }
          logger.error("Caught an exception while trying to fetch a new protocol schema version (" + protocolVersion
              + "). Attempt #" + attempt + "/" + MAX_ATTEMPTS_FOR_SCHEMA_READER + ". Will sleep "
              + WAIT_TIME_BETWEEN_SCHEMA_READER_ATTEMPTS_IN_MS + " ms and try again.", e);
          Utils.sleep(WAIT_TIME_BETWEEN_SCHEMA_READER_ATTEMPTS_IN_MS);
        }
      }
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

      VeniceSpecificDatumReader<SPECIFIC_RECORD> specificDatumReader = readerMap.get(protocolVersion);

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

  /**
   * Initialize both {@link #readerMap} and {@link #writer}.
   *
   * @param protocolDef
   */
  private SpecificDatumWriter initializeAvroSpecificDatumReaderAndWriter(AvroProtocolDefinition protocolDef) {
    Map<Integer, Schema> protocolSchemaMap = Utils.getAllSchemasFromResources(protocolDef);

    /** Initialize {@link #readerMap} based on known protocol versions */
    protocolSchemaMap.forEach((protocolVersion, protocolSchema) -> cacheDatumReader(protocolVersion, protocolSchema));

    return new SpecificDatumWriter(protocolDef.schema);
  }

  private void cacheDatumReader(int protocolVersion, Schema protocolSchema) {
    VeniceSpecificDatumReader<SPECIFIC_RECORD> datumReader = new VeniceSpecificDatumReader<>(protocolSchema, compiledProtocol);
    this.readerMap.put(protocolVersion, datumReader);
  }

  private String getCurrentlyLoadedProtocolVersions() {
    return "[" + readerMap.keySet()
        .stream()
        .sorted()
        .map(b -> b.toString())
        .collect(Collectors.joining(", ")) + "]";
  }
}
