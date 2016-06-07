package com.linkedin.venice.serialization;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceMessageException;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.utils.ByteUtils;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Serializer for the Avro-based kafka protocol defined in:
 *
 * {@link com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope}
 *
 * The protocol is the following:
 *
 * 1st byte: The magic byte, should always equal '{@value #MAGIC_BYTE_VALUE}'.
 * 2nd byte: The protocol version, currently, only '1' is supported.
 * 3rd byte and onward: The payload (a single binary-encoded Avro record) encoded
 *    with a writer schema determined by the protocol version specified in #2.
 */
public class KafkaValueSerializer implements VeniceSerializer<KafkaMessageEnvelope> {

  private static final Logger logger = Logger.getLogger(KafkaValueSerializer.class);

  // Constants related to the protocol definition:

  // 1st byte: Magic Byte
  private static final int MAGIC_BYTE_OFFSET = 0;
  private static final int MAGIC_BYTE_LENGTH = 1;
  private static final byte MAGIC_BYTE_VALUE = (byte) 23;

  // 2nd byte: Protocol version
  private static final int PROTOCOL_VERSION_OFFSET = MAGIC_BYTE_OFFSET + MAGIC_BYTE_LENGTH;
  private static final int PROTOCOL_VERSION_LENGTH = 1;
  private static final byte LATEST_PROTOCOL_VERSION_VALUE = (byte) 1;

  // 3rd byte and onward: Payload (a single binary-encoded Avro record)
  private static final int PAYLOAD_OFFSET = PROTOCOL_VERSION_OFFSET + PROTOCOL_VERSION_LENGTH;

  // Re-usable Avro facilities. Can be shared across multiple threads, so we only need one per process.

  /** Used to generate decoders. */
  private static final DecoderFactory DECODER_FACTORY = new DecoderFactory();

  /** Used to serialize objects into binary-encoded Avro according to the latest protocol version. */
  private static final SpecificDatumWriter SPECIFIC_DATUM_WRITER = new SpecificDatumWriter(KafkaMessageEnvelope.SCHEMA$);

  /** Holds the mapping of protocol version to {@link Schema} instance. */
  private static final Map<Byte, Schema> PROTOCOLS = initializeProtocolSchemaMap();

  public KafkaValueSerializer() {}

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
   * @param object A {@link KafkaMessageEnvelope} instance to be serialized.
   * @return The Avro binary format bytes which represent the {@param object}
   */
  @Override
  public byte[] serialize(String topic, KafkaMessageEnvelope object) {
    try {
      // If single-threaded, both the ByteArrayOutputStream and Encoder can be re-used. TODO: explore GC tuning later.
      ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
      Encoder encoder = new BinaryEncoder(byteArrayOutputStream);

      // We write according to the latest protocol version.
      byteArrayOutputStream.write(MAGIC_BYTE_VALUE);
      byteArrayOutputStream.write(LATEST_PROTOCOL_VERSION_VALUE);
      SPECIFIC_DATUM_WRITER.write(object, encoder);

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
   * @return A {@link KafkaMessageEnvelope} serialized from the bytes
   */
  @Override
  public KafkaMessageEnvelope deserialize(String topic, byte[] bytes) {
    try {
      // Sanity check on the magic byte to make sure we understand the protocol itself
      if (bytes[MAGIC_BYTE_OFFSET] != MAGIC_BYTE_VALUE) {
        throw new VeniceMessageException("Received Magic Byte '" +
            new String(bytes, MAGIC_BYTE_OFFSET, MAGIC_BYTE_LENGTH) +
            "' which is not supported by " + this.getClass().getSimpleName() +
            ". The only supported Magic Byte for this implementation is '" + MAGIC_BYTE_VALUE + "'.");
      }

      // Sanity check to make sure the writer's protocol (i.e.: Avro schema) version is known to us
      if (!PROTOCOLS.containsKey(bytes[PROTOCOL_VERSION_OFFSET])) {
        throw new VeniceMessageException("Received Protocol Version '" +
            new String(bytes, PROTOCOL_VERSION_OFFSET, PROTOCOL_VERSION_LENGTH) +
            "' which is not supported by " + this.getClass().getSimpleName() +
            ". The only supported Protocol Versions are [" + LATEST_PROTOCOL_VERSION_VALUE + "].");
      }

      // If the data looks valid, then we deploy the Avro machinery to decode the payload

      // If single-threaded, DatumReader can be re-used. TODO: explore GC tuning later.
      SpecificDatumReader<KafkaMessageEnvelope> specificDatumReader = new SpecificDatumReader<>();

      specificDatumReader.setSchema(PROTOCOLS.get(bytes[PROTOCOL_VERSION_OFFSET])); // Writer's schema
      specificDatumReader.setExpected(KafkaMessageEnvelope.SCHEMA$);                // Reader's schema

      Decoder decoder = DECODER_FACTORY.createBinaryDecoder(
          bytes,                         // The bytes array we wish to decode
          PAYLOAD_OFFSET,                // Where to start reading from in the bytes array
          bytes.length - PAYLOAD_OFFSET, // The length to read in the bytes array
          null                           // This param is to re-use a Decoder instance. TODO: explore GC tuning later.
      );

      KafkaMessageEnvelope kafkaMessageEnvelope = specificDatumReader.read(
          null, // This param is to re-use a KafkaMessageEnvelope instance. TODO: explore GC tuning later.
          decoder
      );

      return kafkaMessageEnvelope;
    } catch (IOException e) {
      throw new VeniceMessageException("Failed to decode message from topic '" + topic + "': " + ByteUtils.toHexString(bytes), e);
    }
  }

  /**
   * We initialize the protocols map once per process, the first time we construct an instance of this class.
   */
  private static Map<Byte, Schema> initializeProtocolSchemaMap() {
    try {
      Map protocolSchemaMap = new HashMap<>();
      protocolSchemaMap.put((byte) 1, getSchemaFromResource("avro/v1/KafkaValueEnvelope.avsc"));

      // TODO: If we add more versions to the protocol, they should be initialized here.

      return protocolSchemaMap;
    } catch (IOException e) {
      throw new VeniceMessageException("Could not initialize " + KafkaValueSerializer.class.getSimpleName(), e);
    }
  }

  /**
   * Utility function to get schemas out of embedded resources.
   *
   * @param resourcePath The path of the file under the src/main/resources directory
   * @return the {@link org.apache.avro.Schema} instance corresponding to the file at {@param resourcePath}
   * @throws IOException
   */
  private static Schema getSchemaFromResource(String resourcePath) throws IOException {
    ClassLoader classLoader = KafkaValueSerializer.class.getClassLoader();
    InputStream inputStream = classLoader.getResourceAsStream(resourcePath);
    if (inputStream == null) {
      throw new IOException("Resource path '" + resourcePath + "' does not exist!");
    }
    String schemaString = IOUtils.toString(inputStream);
    Schema schema = Schema.parse(schemaString);
    logger.info("Loaded schema from resource path '" + resourcePath + "':\n" + schema.toString(true));
    return schema;
  }
}
